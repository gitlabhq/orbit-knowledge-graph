//! A typed pipeline: `Pipeline<T>` carries one artifact, and `then` swaps
//! it for the next by running a `Phase`. Which phases apply is decided by
//! the artifact's type, so a caller cannot resolve before linking or export
//! before display. `index` and `reindex` are the two common orders; the
//! runner itself knows nothing about files, trees, or graphs.

pub mod phases;
mod state;

pub use phases::*;
pub use state::State;

use std::borrow::Cow;
use std::time::{Duration, Instant};

use crate::env::Env;
use crate::error::Error;
use crate::sentinel::{Killed, Sentinel};

/// Every parseable source through parse, rewrite, link and cross-file resolution.
pub fn index<'e>(
    context: Context<'e>,
    sources: Vec<SourceFile>,
) -> Result<Pipeline<'e, Resolved>, Error> {
    Pipeline::new(context, sources)
        .then(Prepare)?
        .then(Each(Parse.pipe(Rewrite).pipe(Canonicalize).pipe(Link)))?
        .then(Insert)?
        .then(Resolve)
}

/// Only the changed files go through the per-file phases; resolution
/// revisits them and everything that depended on what they replaced.
pub fn reindex<'e>(
    context: Context<'e>,
    state: State,
    changes: Changes,
) -> Result<Pipeline<'e, Resolved>, Error> {
    Pipeline::new(context, ReindexInput { state, changes })
        .then(Remap)?
        .then(Each(Parse.pipe(Rewrite).pipe(Canonicalize).pipe(Link)))?
        .then(Insert)?
        .then(Resolve)
}

pub struct Pipeline<'e, T> {
    context: Context<'e>,
    value: T,
}

impl<'e, T> Pipeline<'e, T> {
    pub fn new(context: Context<'e>, value: T) -> Self {
        Self { context, value }
    }

    pub fn then<P: Phase<T>>(mut self, phase: P) -> Result<Pipeline<'e, P::Output>, Error> {
        let value = self.context.run(phase, self.value)?;
        Ok(Pipeline {
            context: self.context,
            value,
        })
    }

    pub fn value(&self) -> &T {
        &self.value
    }

    pub fn context(&self) -> &Context<'e> {
        &self.context
    }

    pub fn finish(self) -> (Context<'e>, T) {
        (self.context, self.value)
    }

    pub fn into_value(self) -> T {
        self.value
    }
}

/// One step of a pipeline: consumes the current artifact, produces the next.
pub trait Phase<I> {
    type Output;

    fn name(&self) -> Cow<'static, str>;

    fn run(self, context: &mut Context, input: I) -> Result<Self::Output, Error>;
}

/// A step over one item with no shared mutable state, so `phases::Each` can
/// run it across items in parallel and drop an item that overruns its
/// budget without stopping the run. `pipe` fuses two into one pass so an
/// item stays hot through every step.
pub trait ItemPhase<I> {
    type Output;

    fn name(&self) -> Cow<'static, str>;

    fn run(&self, env: &Env, run: &Sentinel, input: I) -> Result<Self::Output, Killed>;

    fn pipe<B: ItemPhase<Self::Output>>(self, next: B) -> Chain<Self, B>
    where
        Self: Sized,
    {
        Chain(self, next)
    }
}

pub struct Chain<A, B>(A, B);

impl<I, A: ItemPhase<I>, B: ItemPhase<A::Output>> ItemPhase<I> for Chain<A, B> {
    type Output = B::Output;

    fn name(&self) -> Cow<'static, str> {
        format!("{}+{}", self.0.name(), self.1.name()).into()
    }

    fn run(&self, env: &Env, run: &Sentinel, input: I) -> Result<Self::Output, Killed> {
        self.1.run(env, run, self.0.run(env, run, input)?)
    }
}

/// What one run shares across its phases. `env` is the language
/// environment, long-lived and shared between runs; `run` is this run's
/// total deadline, checked at every phase boundary and inside hot loops.
pub struct Context<'e> {
    pub env: &'e Env,
    pub run: Sentinel,
    pub report: Report,
    observer: Box<dyn Observer>,
}

impl<'e> Context<'e> {
    pub fn new(env: &'e Env) -> Self {
        Self::with_observer(env, Box::new(NoOpObserver))
    }

    pub fn with_observer(env: &'e Env, observer: Box<dyn Observer>) -> Self {
        Self {
            env,
            run: Sentinel::new("run", "", env.limits.total_ms),
            report: Report::default(),
            observer,
        }
    }

    /// A file that overran its own budget: left out, and reported.
    pub fn skip(&mut self, killed: Killed) {
        self.observer.skipped(&killed);
        self.report.skipped.push(killed);
    }

    pub(super) fn run<I, P: Phase<I>>(&mut self, phase: P, input: I) -> Result<P::Output, Error> {
        let name = phase.name();
        self.observer.started(&name);
        let started = Instant::now();
        let result = self
            .run
            .check()
            .map_err(Error::from)
            .and_then(|()| phase.run(self, input));
        match result {
            Ok(output) => {
                let elapsed = started.elapsed();
                self.observer.finished(&name, elapsed);
                self.report.phases.push(PhaseReport {
                    name: name.into_owned(),
                    elapsed,
                });
                Ok(output)
            }
            Err(error) => {
                self.observer.failed(&name, &error);
                Err(error)
            }
        }
    }
}

#[derive(Default)]
pub struct Report {
    pub skipped: Vec<Killed>,
    pub phases: Vec<PhaseReport>,
}

pub struct PhaseReport {
    pub name: String,
    pub elapsed: Duration,
}

/// Sees the run as it happens. Every method has an empty default, so an
/// observer implements only what it cares about.
pub trait Observer: Send {
    fn started(&mut self, _phase: &str) {}
    fn finished(&mut self, _phase: &str, _elapsed: Duration) {}
    fn skipped(&mut self, _killed: &Killed) {}
    fn failed(&mut self, _phase: &str, _error: &Error) {}
}

pub struct NoOpObserver;

impl Observer for NoOpObserver {}

/// Several observers on one run: progress output, tracing, metrics.
pub type MultiObserver = orbit_utils::observability::MultiObserver<dyn Observer>;

impl Observer for MultiObserver {
    fn started(&mut self, phase: &str) {
        self.iter_mut().for_each(|o| o.started(phase));
    }

    fn finished(&mut self, phase: &str, elapsed: Duration) {
        self.iter_mut().for_each(|o| o.finished(phase, elapsed));
    }

    fn skipped(&mut self, killed: &Killed) {
        self.iter_mut().for_each(|o| o.skipped(killed));
    }

    fn failed(&mut self, phase: &str, error: &Error) {
        self.iter_mut().for_each(|o| o.failed(phase, error));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::{Context, Phase, Pipeline};
    use crate::treesitter::SupportLang;

    struct Log(std::sync::Arc<std::sync::Mutex<Vec<String>>>);

    impl Observer for Log {
        fn started(&mut self, phase: &str) {
            self.0.lock().unwrap().push(format!("start {phase}"));
        }
        fn finished(&mut self, phase: &str, _: Duration) {
            self.0.lock().unwrap().push(format!("finish {phase}"));
        }
    }

    struct Double;

    impl Phase<u32> for Double {
        type Output = u32;
        fn name(&self) -> std::borrow::Cow<'static, str> {
            "double".into()
        }
        fn run(self, _: &mut Context, n: u32) -> Result<u32, Error> {
            Ok(n * 2)
        }
    }

    #[test]
    fn every_observer_sees_every_phase_boundary() {
        let env = crate::Env::for_lang(SupportLang::Python).unwrap();
        let (a, b) = (Default::default(), Default::default());
        let observers = MultiObserver::new(vec![
            Box::new(Log(std::sync::Arc::clone(&a))),
            Box::new(Log(std::sync::Arc::clone(&b))),
        ]);
        let context = Context::with_observer(&env, Box::new(observers));
        let (context, value) = Pipeline::new(context, 3).then(Double).unwrap().finish();
        assert_eq!(value, 6);
        assert_eq!(*a.lock().unwrap(), ["start double", "finish double"]);
        assert_eq!(*a.lock().unwrap(), *b.lock().unwrap());
        assert_eq!(context.report.phases[0].name, "double");
    }
}
