use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug)]
pub struct TickClock {
    tick: Duration,
    now: u64,
}

impl TickClock {
    pub fn new(tick: Duration) -> Self {
        Self { tick, now: 0 }
    }

    pub fn ticks(&self) -> u64 {
        self.now
    }

    pub fn advance(&mut self) {
        self.now += 1;
    }

    pub fn local_now(&self, skew_ticks: u64) -> SystemTime {
        self.time_at(self.now + skew_ticks)
    }

    pub fn time_at(&self, ticks: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(self.tick_millis() * ticks)
    }

    pub fn ticks_at(&self, time: SystemTime) -> u64 {
        let since_epoch = time.duration_since(UNIX_EPOCH).unwrap_or_default();
        u64::try_from(since_epoch.as_millis()).unwrap_or(u64::MAX) / self.tick_millis()
    }

    fn tick_millis(&self) -> u64 {
        u64::try_from(self.tick.as_millis())
            .unwrap_or(u64::MAX)
            .max(1)
    }
}
