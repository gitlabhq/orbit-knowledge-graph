//! `Pipeline<T>` carries one artifact; `then` swaps it for the next by
//! running a `Phase`. The artifact's type decides which phases apply:
//!
//! ```text
//! Sources ─Prepare─> Workset<Lazy<SourceFile>>
//!   ─Each(Parse+Rewrite+Canonicalize+Link)─> Workset<Vec<LinkedFile>>
//!   ─Insert─> DirtyGraph ─Resolve─> Resolved ─Display─> Displayed
//!   ─Export─> Exported ─Emit─> Exported
//! ReindexInput ─Remap─> Workset<Lazy<SourceFile>> ─(as above)
//! ```

mod artifacts;
mod phases;
mod state;

pub use artifacts::*;
pub use phases::*;
pub use state::{SourceFile, State};

use std::borrow::Cow;
use std::time::{Duration, Instant};

use crate::env::Env;
use crate::error::Error;
use crate::sentinel::{Killed, Sentinel};

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

/// A step over one item with no shared state, so items run in parallel.
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

/// What one run shares across its phases; `run` is the run's total deadline.
pub struct Context<'e> {
    pub env: &'e Env,
    pub run: Sentinel,
    pub report: Report,
    observers: Vec<Box<dyn Observer>>,
}

impl<'e> Context<'e> {
    pub fn new(env: &'e Env) -> Self {
        Self {
            env,
            run: Sentinel::new("run", "", env.limits.total_ms),
            report: Report::default(),
            observers: Vec::new(),
        }
    }

    pub fn observe(mut self, observer: impl Observer + 'static) -> Self {
        self.observers.push(Box::new(observer));
        self
    }

    pub fn skip(&mut self, killed: Killed) {
        self.observers.iter_mut().for_each(|o| o.skipped(&killed));
        self.report.skipped.push(killed);
    }

    pub(super) fn run<I, P: Phase<I>>(&mut self, phase: P, input: I) -> Result<P::Output, Error> {
        let name = phase.name();
        self.observers.iter_mut().for_each(|o| o.started(&name));
        let started = Instant::now();
        let result = self
            .run
            .check()
            .map_err(Error::from)
            .and_then(|()| phase.run(self, input));
        match &result {
            Ok(_) => {
                let elapsed = started.elapsed();
                self.observers
                    .iter_mut()
                    .for_each(|o| o.finished(&name, elapsed));
                self.report.phases.push(PhaseReport {
                    name: name.into_owned(),
                    elapsed,
                });
            }
            Err(error) => self
                .observers
                .iter_mut()
                .for_each(|o| o.failed(&name, error)),
        }
        result
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

/// Sees the run as it happens; implement only the methods you need.
pub trait Observer: Send {
    fn started(&mut self, _phase: &str) {}
    fn finished(&mut self, _phase: &str, _elapsed: Duration) {}
    fn skipped(&mut self, _killed: &Killed) {}
    fn failed(&mut self, _phase: &str, _error: &Error) {}
}
