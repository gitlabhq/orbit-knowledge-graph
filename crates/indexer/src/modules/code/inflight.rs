//! Source bytes in flight across one job's parses, admitted in arrival order
//! so a large file waits only for the files already running; a file larger
//! than the budget runs alone.

use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use code_graph_incremental::{Admission, Killed, Sentinel, Unbounded};

const POLL: Duration = Duration::from_millis(50);

pub fn budget(bytes: u64) -> Arc<dyn Admission> {
    if bytes == 0 {
        Arc::new(Unbounded)
    } else {
        Arc::new(InflightBudget::new(bytes))
    }
}

pub struct InflightBudget {
    budget: u64,
    queue: Mutex<Queue>,
    changed: Condvar,
}

#[derive(Default)]
struct Queue {
    used: u64,
    next: u64,
    serving: u64,
}

impl InflightBudget {
    fn new(budget: u64) -> Self {
        Self {
            budget,
            queue: Mutex::default(),
            changed: Condvar::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Queue> {
        self.queue.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Admission for InflightBudget {
    fn admit(&self, bytes: u64, run: &Sentinel) -> Result<(), Killed> {
        let mut queue = self.lock();
        let ticket = queue.next;
        queue.next += 1;
        loop {
            let killed = run.check().err();
            if queue.serving == ticket {
                if let Some(killed) = killed {
                    queue.serving += 1;
                    self.changed.notify_all();
                    return Err(killed);
                }
                if queue.used == 0 || queue.used + bytes <= self.budget {
                    break;
                }
            }
            queue = self
                .changed
                .wait_timeout(queue, POLL)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
        queue.serving += 1;
        queue.used += bytes;
        self.changed.notify_all();
        Ok(())
    }

    fn release(&self, bytes: u64) {
        self.lock().used -= bytes;
        self.changed.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    use super::*;

    const MIB: u64 = 1024 * 1024;

    fn settles(what: &str, condition: impl Fn() -> bool) {
        let started = Instant::now();
        while !condition() {
            assert!(started.elapsed() < Duration::from_secs(10), "{what}");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    struct Held<'a>(&'a InflightBudget, u64);

    impl Drop for Held<'_> {
        fn drop(&mut self) {
            self.0.release(self.1);
        }
    }

    fn hold(budget: &InflightBudget, bytes: u64) -> Held<'_> {
        budget
            .admit(bytes, &Sentinel::disabled())
            .unwrap_or_else(|e| panic!("{e}"));
        Held(budget, bytes)
    }

    #[test]
    fn a_cancelled_waiter_leaves_with_the_kill_and_frees_its_turn() {
        let budget = InflightBudget::new(2 * MIB);
        let first = hold(&budget, 2 * MIB);
        let cancelled = Sentinel::new("run", "", u64::MAX).with_cancellation(Arc::new(|| true));
        let killed = match budget.admit(MIB, &cancelled) {
            Err(killed) => killed,
            Ok(()) => panic!("a cancelled waiter must not be admitted"),
        };
        assert!(killed.cancelled);
        drop(first);
        let _next = hold(&budget, MIB);
    }

    #[test]
    fn admission_is_first_come_first_served_and_an_oversize_item_runs_alone() {
        let budget = InflightBudget::new(2 * MIB);
        let order = Mutex::new(Vec::new());
        let (release_first, first_held) = std::sync::mpsc::channel::<()>();
        let (release_big, big_held) = std::sync::mpsc::channel::<()>();
        std::thread::scope(|scope| {
            let (budget, order) = (&budget, &order);
            scope.spawn(move || {
                let _first = hold(budget, MIB);
                order
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push("first");
                first_held.recv().ok();
            });
            settles("first is admitted", || budget.lock().used == MIB);

            scope.spawn(move || {
                let _big = hold(budget, 3 * MIB);
                order
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push("big");
                big_held.recv().ok();
            });
            settles("big has its ticket", || budget.lock().next == 2);

            scope.spawn(move || {
                let _small = hold(budget, MIB / 4);
                order
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push("small");
            });
            settles("small has its ticket", || budget.lock().next == 3);
            assert_eq!(
                budget.lock().used,
                MIB,
                "small waits behind big instead of slipping past it"
            );

            release_first.send(()).unwrap_or_else(|e| panic!("{e}"));
            settles("big runs alone", || budget.lock().used == 3 * MIB);
            settles("big is recorded", || {
                order.lock().unwrap_or_else(PoisonError::into_inner).len() == 2
            });
            assert_eq!(
                *order.lock().unwrap_or_else(PoisonError::into_inner),
                ["first", "big"]
            );

            release_big.send(()).unwrap_or_else(|e| panic!("{e}"));
            settles("small runs after big", || {
                order.lock().unwrap_or_else(PoisonError::into_inner).len() == 3
            });
        });
        assert_eq!(
            *order.lock().unwrap_or_else(PoisonError::into_inner),
            ["first", "big", "small"]
        );
    }

    /// A barrier that fails instead of hanging when fewer items than expected
    /// are admitted together.
    struct Meet {
        parties: usize,
        state: Mutex<(usize, usize)>,
        changed: Condvar,
    }

    impl Meet {
        fn new(parties: usize) -> Self {
            Self {
                parties,
                state: Mutex::new((0, 0)),
                changed: Condvar::new(),
            }
        }

        fn wait(&self) {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let generation = state.1;
            state.0 += 1;
            if state.0 == self.parties {
                *state = (0, generation + 1);
                self.changed.notify_all();
                return;
            }
            let deadline = Instant::now() + Duration::from_secs(10);
            while state.1 == generation {
                let left = deadline.saturating_duration_since(Instant::now());
                assert!(
                    !left.is_zero(),
                    "only {} of {} items were admitted together",
                    state.0,
                    self.parties
                );
                state = self
                    .changed
                    .wait_timeout(state, left)
                    .unwrap_or_else(PoisonError::into_inner)
                    .0;
            }
        }
    }

    fn peak_inflight(sizes: &[u64], budget: &InflightBudget, parties: usize) -> u64 {
        let meet = Meet::new(parties);
        let inflight = AtomicU64::new(0);
        let peak = AtomicU64::new(0);
        std::thread::scope(|scope| {
            for &bytes in sizes {
                let (meet, inflight, peak) = (&meet, &inflight, &peak);
                scope.spawn(move || {
                    let _held = hold(budget, bytes);
                    let now = inflight.fetch_add(bytes, Ordering::Relaxed) + bytes;
                    peak.fetch_max(now, Ordering::Relaxed);
                    meet.wait();
                    inflight.fetch_sub(bytes, Ordering::Relaxed);
                });
            }
        });
        peak.load(Ordering::Relaxed)
    }

    #[test]
    fn bytes_in_flight_stay_under_the_budget_and_an_oversize_item_runs_alone() {
        let eight = [MIB; 8];
        assert_eq!(
            peak_inflight(&eight, &InflightBudget::new(8 * MIB), 8),
            8 * MIB
        );
        assert_eq!(
            peak_inflight(&eight, &InflightBudget::new(2 * MIB), 2),
            2 * MIB
        );
        assert_eq!(
            peak_inflight(&[3 * MIB], &InflightBudget::new(2 * MIB), 1),
            3 * MIB
        );
    }

    #[test]
    fn a_zero_budget_is_unbounded() {
        let admission = budget(0);
        admission
            .admit(u64::MAX, &Sentinel::disabled())
            .unwrap_or_else(|e| panic!("{e}"));
        admission.release(u64::MAX);
    }
}
