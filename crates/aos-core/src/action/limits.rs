//! Local provider retention limits; these are not wire QoS or goal states.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    fmt, io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

/// Limits on one ActionServer's retained business data.
///
/// Byte counts use JSON UTF-8 for goal/result values and UTF-8 for error text,
/// excluding bounded protocol metadata. They are not measurements of heap RSS.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ActionResourceLimits {
    /// Pending admission objects and registered goals share this limit.
    pub max_goals: usize,
    pub max_goal_bytes: usize,
    pub max_result_bytes: usize,
    /// Shared result values keep their reservation while a reply still uses
    /// them, even after the registry releases its cache reference.
    pub max_result_cache_bytes: usize,
}

impl Default for ActionResourceLimits {
    fn default() -> Self {
        Self {
            max_goals: 1024,
            max_goal_bytes: 1 << 20,
            max_result_bytes: 1 << 20,
            max_result_cache_bytes: 64 << 20,
        }
    }
}

impl ActionResourceLimits {
    pub fn validate(&self) -> Result<()> {
        for (name, value, maximum) in [
            ("max_goals", self.max_goals, 65_536),
            ("max_goal_bytes", self.max_goal_bytes, 8 << 20),
            ("max_result_bytes", self.max_result_bytes, 8 << 20),
            (
                "max_result_cache_bytes",
                self.max_result_cache_bytes,
                1 << 30,
            ),
        ] {
            if !(1..=maximum).contains(&value) {
                return Err(Error::InvalidArgument(format!(
                    "{name} must be an integer in 1..={maximum}"
                )));
            }
        }
        if self.max_result_cache_bytes < self.max_result_bytes {
            return Err(Error::InvalidArgument(
                "max_result_cache_bytes must be >= max_result_bytes".into(),
            ));
        }
        Ok(())
    }

    /// Resolve language-binding input with the same defaults and validation.
    /// Omitted object fields use defaults; unknown fields are rejected.
    pub fn from_json(value: &Value) -> Result<Self> {
        let limits: Self = if value.is_null() {
            Self::default()
        } else {
            if !value.is_object() {
                return Err(Error::InvalidArgument(
                    "Action resource_limits must be an object".into(),
                ));
            }
            serde_json::from_value(value.clone()).map_err(|error| {
                Error::InvalidArgument(format!("invalid Action resource_limits: {error}"))
            })?
        };
        limits.validate()?;
        Ok(limits)
    }
}

pub(super) struct CountBudget {
    pub limit: usize,
    pub used: AtomicUsize,
}
impl CountBudget {
    pub fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: AtomicUsize::new(0),
        })
    }
    pub fn acquire(self: &Arc<Self>) -> Option<CountPermit> {
        let mut used = self.used.load(Ordering::Acquire);
        loop {
            let next = used.checked_add(1).filter(|next| *next <= self.limit)?;
            match self
                .used
                .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Some(CountPermit(self.clone())),
                Err(current) => used = current,
            }
        }
    }
}
pub(super) struct CountPermit(Arc<CountBudget>);
impl Drop for CountPermit {
    fn drop(&mut self) {
        let previous = self.0.used.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0);
    }
}

pub(super) struct ByteBudget {
    limit: usize,
    pub used: AtomicUsize,
}
impl ByteBudget {
    pub fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: AtomicUsize::new(0),
        })
    }
    pub fn acquire(self: &Arc<Self>, bytes: usize) -> Option<BytePermit> {
        let mut used = self.used.load(Ordering::Acquire);
        loop {
            let next = used.checked_add(bytes).filter(|next| *next <= self.limit)?;
            match self
                .used
                .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Some(BytePermit(self.clone(), bytes)),
                Err(current) => used = current,
            }
        }
    }
}
pub(super) struct BytePermit(Arc<ByteBudget>, usize);
impl Drop for BytePermit {
    fn drop(&mut self) {
        let previous = self.0.used.fetch_sub(self.1, Ordering::AcqRel);
        debug_assert!(previous >= self.1);
    }
}

struct SizeWriter {
    limit: usize,
    used: usize,
    exceeded: bool,
}
impl SizeWriter {
    fn add(&mut self, bytes: usize) -> bool {
        match self.used.checked_add(bytes) {
            Some(next) if next <= self.limit => {
                self.used = next;
                true
            }
            _ => {
                self.exceeded = true;
                false
            }
        }
    }
}
impl io::Write for SizeWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.add(bytes.len()) {
            Ok(bytes.len())
        } else {
            Err(io::Error::other("Action serialized value exceeds limit"))
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl fmt::Write for SizeWriter {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        self.add(value.len()).then_some(()).ok_or(fmt::Error)
    }
}

/// Count without allocating an encoded copy; stop as soon as the limit fails.
pub(super) fn json_size(value: &Value, limit: usize) -> Result<Option<usize>> {
    let mut writer = SizeWriter {
        limit,
        used: 0,
        exceeded: false,
    };
    match serde_json::to_writer(&mut writer, value) {
        Ok(()) => Ok(Some(writer.used)),
        Err(_) if writer.exceeded => Ok(None),
        Err(error) => Err(Error::Validation(format!(
            "Action value could not be serialized: {error}"
        ))),
    }
}

pub(super) fn text_size(value: fmt::Arguments<'_>, limit: usize) -> Option<usize> {
    let mut writer = SizeWriter {
        limit,
        used: 0,
        exceeded: false,
    };
    fmt::write(&mut writer, value).ok().map(|()| writer.used)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn limits_share_strict_positive_integer_validation_and_defaults() {
        assert_eq!(
            ActionResourceLimits::from_json(&json!({})).unwrap(),
            ActionResourceLimits::default()
        );
        for (name, maximum) in [
            ("max_goals", 65_536_u64),
            ("max_goal_bytes", 8 << 20),
            ("max_result_bytes", 8 << 20),
            ("max_result_cache_bytes", 1 << 30),
        ] {
            for value in [
                json!(0),
                json!(-1),
                json!(maximum + 1),
                json!(u64::MAX),
                json!(true),
                json!(1.0),
                json!("1"),
                Value::Null,
            ] {
                assert!(
                    matches!(
                        ActionResourceLimits::from_json(&json!({name:value})),
                        Err(Error::InvalidArgument(_))
                    ),
                    "{name} unexpectedly accepted {value}"
                );
            }
        }
        for value in [
            json!([]),
            json!(true),
            json!(1),
            json!({"unknown":1}),
            json!({"max_result_bytes":2,"max_result_cache_bytes":1}),
        ] {
            assert!(ActionResourceLimits::from_json(&value).is_err());
        }
        for value in [
            json!({"max_goals":1,"max_goal_bytes":1,
                "max_result_bytes":1,"max_result_cache_bytes":1}),
            json!({"max_goals":65536,"max_goal_bytes":8<<20,
                "max_result_bytes":8<<20,"max_result_cache_bytes":1<<30}),
        ] {
            assert!(ActionResourceLimits::from_json(&value).is_ok());
        }
    }

    #[test]
    fn bounded_size_counts_json_escaping_and_utf8_at_the_exact_boundary() {
        for value in [
            json!(""),
            json!("汉字\n\"\\\u{0}🙂"),
            json!({"nested":[1,true,null,"é"]}),
        ] {
            let size = serde_json::to_vec(&value).unwrap().len();
            assert_eq!(json_size(&value, size).unwrap(), Some(size));
            assert_eq!(json_size(&value, size - 1).unwrap(), None);
        }
        assert_eq!(text_size(format_args!("{}:{}", "汉", "🙂"), 8), Some(8));
        assert_eq!(text_size(format_args!("{}:{}", "汉", "🙂"), 7), None);
        let mut writer = SizeWriter {
            limit: usize::MAX,
            used: usize::MAX,
            exceeded: false,
        };
        assert!(!writer.add(1));
        assert!(writer.exceeded);
    }

    #[test]
    fn permits_use_checked_arithmetic_and_release_their_own_reservation() {
        let budget = ByteBudget::new(usize::MAX);
        let all = budget.acquire(usize::MAX).unwrap();
        assert!(budget.acquire(1).is_none());
        drop(all);
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
        let one = budget.acquire(1).unwrap();
        let rest = budget.acquire(usize::MAX - 1).unwrap();
        drop(one);
        assert_eq!(budget.used.load(Ordering::Acquire), usize::MAX - 1);
        drop(rest);
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
        let count = CountBudget::new(usize::MAX);
        count.used.store(usize::MAX, Ordering::Release);
        assert!(count.acquire().is_none());
    }

    #[test]
    fn concurrent_reservations_never_exceed_the_budget() {
        let budget = ByteBudget::new(7);
        let start = std::sync::Barrier::new(17);
        std::thread::scope(|scope| {
            for _ in 0..16 {
                let start = &start;
                let budget = &budget;
                scope.spawn(move || {
                    start.wait();
                    for _ in 0..100 {
                        let permit = budget.acquire(3);
                        assert!(budget.used.load(Ordering::Acquire) <= 7);
                        std::thread::yield_now();
                        drop(permit);
                    }
                });
            }
            start.wait();
        });
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
    }
}
