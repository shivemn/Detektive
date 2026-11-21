//! Replay speed: how event time maps onto wall-clock time.
//!
//! At speed 100, ten minutes between two transactions in the data become six
//! seconds of real waiting. At `max` nothing waits, which is what benchmarks
//! use.

use std::str::FromStr;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Speed {
    Max,
    Factor(f64),
}

impl FromStr for Speed {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("max") {
            return Ok(Self::Max);
        }
        match s.parse::<f64>() {
            Ok(f) if f.is_finite() && f > 0.0 => Ok(Self::Factor(f)),
            _ => Err(format!(
                "speed must be a positive number or \"max\", got {s:?}"
            )),
        }
    }
}

pub struct Pacer {
    speed: Speed,
    // first event's timestamp and the moment it was released
    origin: Option<(i64, Instant)>,
}

impl Pacer {
    pub fn new(speed: Speed) -> Self {
        Self {
            speed,
            origin: None,
        }
    }

    /// Block until the event at `event_ts` (seconds) is due.
    pub fn wait_for(&mut self, event_ts: i64) {
        let (origin_ts, origin_wall) = *self.origin.get_or_insert((event_ts, Instant::now()));
        let delay = self.delay(event_ts - origin_ts, origin_wall.elapsed());
        if !delay.is_zero() {
            thread::sleep(delay);
        }
    }

    /// How long to wait before releasing an event `event_offset` seconds after
    /// the first one, when `elapsed` real time has already passed. Pure, so it
    /// can be tested without sleeping.
    pub fn delay(&self, event_offset: i64, elapsed: Duration) -> Duration {
        match self.speed {
            Speed::Max => Duration::ZERO,
            Speed::Factor(f) => {
                let due = Duration::from_secs_f64(event_offset.max(0) as f64 / f);
                due.saturating_sub(elapsed)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_speeds() {
        assert_eq!("max".parse::<Speed>(), Ok(Speed::Max));
        assert_eq!("MAX".parse::<Speed>(), Ok(Speed::Max));
        assert_eq!("100".parse::<Speed>(), Ok(Speed::Factor(100.0)));
        assert_eq!("0.5".parse::<Speed>(), Ok(Speed::Factor(0.5)));
        for bad in ["0", "-3", "fast", "inf", "NaN"] {
            assert!(bad.parse::<Speed>().is_err(), "{bad} should be rejected");
        }
    }

    #[test]
    fn ten_minutes_at_speed_100_is_six_seconds() {
        let p = Pacer::new(Speed::Factor(100.0));
        assert_eq!(p.delay(600, Duration::ZERO), Duration::from_secs(6));
        assert_eq!(p.delay(600, Duration::from_secs(4)), Duration::from_secs(2));
    }

    #[test]
    fn never_waits_when_behind_or_at_max() {
        let p = Pacer::new(Speed::Factor(1.0));
        assert_eq!(p.delay(60, Duration::from_secs(90)), Duration::ZERO);
        assert_eq!(
            Pacer::new(Speed::Max).delay(1_000_000, Duration::ZERO),
            Duration::ZERO
        );
    }

    #[test]
    fn first_event_is_released_immediately() {
        let mut p = Pacer::new(Speed::Factor(1.0));
        let start = Instant::now();
        p.wait_for(1_661_991_600);
        assert!(start.elapsed() < Duration::from_millis(50));
    }
}
