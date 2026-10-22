//! Bounded statistics over adapter delivery facts, never dequeue timestamps.
use std::collections::VecDeque;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use aos_core::{Sample, SourceMetadata};
use serde_json::{json, Value};

pub struct Statistics {
    window: usize,
    recent: VecDeque<(Option<Instant>, usize)>,
    pub total_samples: u64,
    pub total_payload_bytes: u64,
    missing_receive_metadata: u64,
    last_source: Option<SourceMetadata>,
}

impl Statistics {
    pub fn new(window: usize) -> Self {
        Self {
            window,
            recent: VecDeque::new(),
            total_samples: 0,
            total_payload_bytes: 0,
            missing_receive_metadata: 0,
            last_source: None,
        }
    }

    pub fn record(&mut self, sample: &Sample<Value>) {
        self.total_samples += 1;
        self.total_payload_bytes = self
            .total_payload_bytes
            .saturating_add(sample.payload_size_bytes as u64);
        self.missing_receive_metadata += u64::from(sample.received.is_none());
        self.last_source.clone_from(&sample.source);
        self.recent.push_back((
            sample.received.map(|received| received.steady),
            sample.payload_size_bytes,
        ));
        if self.recent.len() > self.window {
            self.recent.pop_front();
        }
    }

    pub fn report(&self) -> Value {
        let unavailable = |reason| json!({"status":"unavailable","reason":reason});
        let mut report = json!({
            "scope":"selected messages at adapter delivery; includes history/recovery deliveries",
            "time_domain":"steady",
            "configured_window":self.window,"window_samples":self.recent.len(),
            "selected_total":self.total_samples,"total_payload_bytes":self.total_payload_bytes,
            "missing_receive_metadata":self.missing_receive_metadata,
            "bytes_scope":"original serialized business payload; excludes transport framing and network overhead",
            "hz":unavailable("at least two delivery timestamps are required"),
            "bw":unavailable("at least two delivery timestamps are required"),
            "delay":self.delay(),
            "source":self.last_source,
        });
        if self.recent.is_empty() {
            return report;
        }
        let bytes: usize = self.recent.iter().map(|(_, bytes)| bytes).sum();
        report["payload_size"] = json!({
            "mean_bytes":bytes as f64 / self.recent.len() as f64,
            "min_bytes":self.recent.iter().map(|(_, bytes)| bytes).min(),
            "max_bytes":self.recent.iter().map(|(_, bytes)| bytes).max(),
            "window_payload_bytes":bytes,
        });
        if self.recent.len() < 2 {
            return report;
        }
        let mut intervals = Vec::with_capacity(self.recent.len() - 1);
        for (left, right) in self.recent.iter().zip(self.recent.iter().skip(1)) {
            let Some((before, after)) = left.0.zip(right.0) else {
                report["hz"] = unavailable("receive metadata is missing in this window");
                report["bw"] = report["hz"].clone();
                return report;
            };
            let Some(interval) = after.checked_duration_since(before) else {
                report["hz"] = unavailable("delivery timestamps are out of order in this window");
                report["bw"] = report["hz"].clone();
                return report;
            };
            intervals.push(interval.as_secs_f64());
        }
        let span: f64 = intervals.iter().sum();
        if span == 0.0 {
            report["hz"] = unavailable("delivery timestamps have no measurable span");
            report["bw"] = report["hz"].clone();
            return report;
        }
        let mean = span / intervals.len() as f64;
        let variance =
            intervals.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / intervals.len() as f64;
        report["hz"] = json!({
            "status":"available","value":1.0 / mean,"window_seconds":span,
            "interval_mean_seconds":mean,
            "interval_min_seconds":intervals.iter().copied().reduce(f64::min),
            "interval_max_seconds":intervals.iter().copied().reduce(f64::max),
            "interval_stddev_seconds":variance.sqrt(),
        });
        // The first sample anchors the window; only subsequent payloads arrived
        // during its measured intervals. Including its bytes would bias short
        // windows (two samples at 10 Hz would otherwise report twice the rate).
        let rate_bytes: usize = self.recent.iter().skip(1).map(|(_, bytes)| bytes).sum();
        report["bw"] = json!({
            "status":"available","payload_bytes_per_second":rate_bytes as f64 / span,
            "window_seconds":span,"interval_payload_bytes":rate_bytes,
            "boundary":"first sample anchors time; its payload is excluded from interval bytes",
        });
        report
    }

    fn delay(&self) -> Value {
        let timestamp = self
            .last_source
            .as_ref()
            .and_then(|source| source.timestamp.as_ref());
        let reason = match timestamp {
            None => "source publication timestamp is unavailable",
            Some(timestamp) if !timestamp.comparable =>
                "source clock comparability is not established; router timestamps may be injected or rewritten",
            Some(_) =>
                "the adapter timestamp domain has no validated mapping to local system time",
        };
        // Zenoh HLC/NTP64 is not automatically a synchronized publication time.
        // Do not subtract it from wall time merely because both are numeric.
        json!({"status":"unavailable","reason":reason,"source_timestamp":timestamp})
    }
}

pub fn system_time_ns(time: SystemTime) -> Option<i64> {
    match time.duration_since(UNIX_EPOCH) {
        Ok(value) => i64::try_from(value.as_nanos()).ok(),
        Err(error) => i64::try_from(error.duration().as_nanos())
            .ok()?
            .checked_neg(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aos_core::{ReceiveMetadata, ReceiveScope, SourceTimestamp};
    use std::time::Duration;

    fn sample(time: Instant, bytes: usize) -> Sample<Value> {
        Sample {
            payload: json!({}),
            payload_size_bytes: bytes,
            received: Some(ReceiveMetadata {
                steady: time,
                system: UNIX_EPOCH,
                scope: ReceiveScope::AdapterDelivery,
            }),
            source: None,
        }
    }

    #[test]
    fn rolling_window_uses_delivery_intervals_and_original_payload_sizes() {
        let start = Instant::now();
        let mut stats = Statistics::new(3);
        for index in 0..4 {
            stats.record(&sample(
                start + Duration::from_millis(index * 100),
                (index as usize + 1) * 10,
            ));
        }
        let result = stats.report();
        assert_eq!(result["window_samples"], 3);
        assert_eq!(result["total_payload_bytes"], 100);
        assert_eq!(result["hz"]["value"], 10.0);
        assert_eq!(result["bw"]["payload_bytes_per_second"], 350.0);
        assert_eq!(result["payload_size"]["window_payload_bytes"], 90);
    }

    #[test]
    fn missing_or_reversed_delivery_times_are_not_replaced_with_dequeue_time() {
        let start = Instant::now();
        let mut stats = Statistics::new(2);
        stats.record(&sample(start + Duration::from_secs(1), 10));
        stats.record(&sample(start, 10));
        assert_eq!(stats.report()["hz"]["status"], "unavailable");
        let mut missing = sample(start, 10);
        missing.received = None;
        stats.record(&missing);
        assert_eq!(stats.report()["bw"]["status"], "unavailable");
    }

    #[test]
    fn router_hlc_does_not_become_a_cross_machine_delay() {
        let mut value = sample(Instant::now(), 10);
        value.source = Some(SourceMetadata {
            publisher_id: None,
            sequence: None,
            timestamp: Some(SourceTimestamp {
                kind: "zenoh_hlc".into(),
                value: "18446744073709551615".into(),
                clock_id: "router".into(),
                comparable: false,
                origin: "router_or_publisher".into(),
            }),
        });
        let mut stats = Statistics::new(2);
        stats.record(&value);
        let result = stats.report();
        assert_eq!(result["delay"]["status"], "unavailable");
        assert!(!result["delay"].as_object().unwrap().contains_key("value"));
    }
}
