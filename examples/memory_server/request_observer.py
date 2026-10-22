"""Export real Core request records, or observe their Topic without business calls.

Importing this module opens no session and does not import the native SDK.
Only the source process can drain its Context. Capture is opt-in and has one
owner; the independent publisher Context never captures its own diagnostics.
"""
from __future__ import annotations

import argparse
from collections import deque
from contextlib import nullcontext
import json
import math
from pathlib import Path
import re
import sys
import threading
import time
import unicodedata
import uuid

MAX_BATCH_BYTES = 128 * 1024
MAX_BATCH_RECORDS = 32
UINT64 = 2**64 - 1
SOURCE_COUNTERS = (
    "events_total", "rejected_events_total", "metric_keys_dropped_total",
    "log_dropped_total", "log_write_failures_total", "log_capacity", "log_queued",
)
EXPORT_COUNTERS = (
    "drained_records_total", "filtered_records_total", "invalid_records_total",
    "queue_dropped_records_total", "queue_dropped_bytes_total", "oversized_records_total",
    "publish_attempts_total", "publish_submitted_total", "publish_errors_total",
    "publish_failed_records_total", "source_errors_total", "export_capture_errors_total",
)
OUTCOMES = (
    "ok", "rejected", "inactive", "closed", "timeout", "invalid_argument",
    "resource_limit", "validation", "unsupported", "remote", "queue_overflow",
    "no_response", "transport",
)
CORRELATIONS = (
    "outgoing_attachment", "incoming_attachment", "reply_attachment", "query_context",
    "missing_attachment", "invalid_attachment", "mismatched_attachment",
)
_OWNER_LOCK = threading.Lock()
_OWNERS = set()


def encoded(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")


def _uint(value, maximum=UINT64):
    return type(value) is int and 0 <= value <= maximum


def _matches(value, pattern, maximum):
    return isinstance(value, str) and len(value) <= maximum and re.fullmatch(pattern, value) is not None


def validate_record(record):
    """Bound and validate metadata without filling, renaming or reconstructing it."""
    required = {"schema_version", "process_id", "context_id", "sequence", "system_time_unix_ns",
                "steady_time_ns", "identity_trust", "component", "event", "outcome"}
    optional = {"node_id", "endpoint", "request_id", "goal_id", "correlation_source", "bytes", "duration_ns"}
    if not isinstance(record, dict) or not required <= record.keys() or record.keys() - required - optional:
        raise ValueError("invalid record fields")
    if (type(record["schema_version"]) is not int or record["schema_version"] != 1
            or not _uint(record["process_id"], 2**32 - 1)
            or not _matches(record["context_id"], r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", 36)
            or not _uint(record["sequence"]) or not _uint(record["steady_time_ns"])
            or record["identity_trust"] != "unauthenticated"
            or record["component"] not in ("service", "action")
            or not _matches(record["event"], r"[a-z0-9_.]+", 64)
            or record["outcome"] not in OUTCOMES):
        raise ValueError("invalid record metadata")
    wall = record["system_time_unix_ns"]
    if wall is not None and not _matches(wall, r"[0-9]+", 39):
        raise ValueError("invalid record wall time")
    for key in ("node_id", "request_id", "goal_id"):
        if key in record and not _matches(record[key], r"[A-Za-z0-9_:.\-]+", 128):
            raise ValueError("invalid record identifier")
    if "endpoint" in record:
        value = record["endpoint"]
        if (not isinstance(value, str) or not value or len(value.encode("utf-8")) > 512
                or any(unicodedata.category(char) == "Cc" for char in value)):
            raise ValueError("invalid record endpoint")
    if "correlation_source" in record and record["correlation_source"] not in CORRELATIONS:
        raise ValueError("invalid record correlation")
    if any(key in record and not _uint(record[key]) for key in ("bytes", "duration_ns")):
        raise ValueError("invalid record counter")
    return record


def _source_status(snapshot):
    value = {name: snapshot[name] for name in SOURCE_COUNTERS}
    value["log_capture_enabled"] = snapshot["log_capture_enabled"]
    if any(not _uint(value[name]) for name in SOURCE_COUNTERS) or type(value["log_capture_enabled"]) is not bool:
        raise ValueError("invalid source counters")
    return value


def validate_batch(batch):
    fields = {"schema_version", "stream_id", "batch_sequence", "source_context_id", "source_process_id",
              "source_before", "source_after", "statistics", "records"}
    if not isinstance(batch, dict) or batch.keys() != fields or len(encoded(batch)) > MAX_BATCH_BYTES:
        raise ValueError("invalid batch fields or bytes")
    if (type(batch["schema_version"]) is not int or batch["schema_version"] != 1
            or not _matches(batch["stream_id"], r"[0-9a-f]{32}", 32)
            or not _uint(batch["batch_sequence"]) or batch["batch_sequence"] < 1
            or not _matches(batch["source_context_id"], r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", 36)
            or not _uint(batch["source_process_id"], 2**32 - 1)):
        raise ValueError("invalid batch identity")
    for name in ("source_before", "source_after"):
        value = batch[name]
        if not isinstance(value, dict) or value.keys() != set(SOURCE_COUNTERS) | {"log_capture_enabled"}:
            raise ValueError("invalid source counter fields")
        _source_status(value)
    statistics = batch["statistics"]
    if (not isinstance(statistics, dict) or statistics.keys() != set(EXPORT_COUNTERS)
            or any(not _uint(value) for value in statistics.values())):
        raise ValueError("invalid export counters")
    if not isinstance(batch["records"], list) or len(batch["records"]) > MAX_BATCH_RECORDS:
        raise ValueError("invalid batch record count")
    for record in batch["records"]:
        validate_record(record)
        if (record["context_id"] != batch["source_context_id"]
                or record["process_id"] != batch["source_process_id"]):
            raise ValueError("record source differs from batch")
    return batch


def check_executors(executors):
    for executor in executors:
        # This example is bound to the accompanying SDK executor implementation.
        # Unknown executor types must explicitly expose the same false setting.
        if getattr(executor, "_flush_logs_on_spin", None) is not False:
            raise ValueError("request telemetry requires known executors with flush_logs=False")


def _interface(aos, directory):
    return aos.Interface.load(Path(directory) / "RequestTelemetry.json" if directory is not None
                              else aos.get_package_resource("aos_memory", "request_telemetry"))


class _TopicTransport:
    def __init__(self, *, topic, name, namespace, context_options, interface_directory):
        import aos_sdk as aos
        self.context = aos.Context(**context_options)
        self.node = None
        try:
            self.context.disable_log_capture()
            self.node = aos.Node(name, namespace, self.context, start_parameter_services=False,
                summary="Read-only Core request diagnostics exporter; does not execute recorded requests.",
                tags=("diagnostics", "request_observer"))
            self.publisher = self.node.create_publisher(_interface(aos, interface_directory), topic,
                qos_profile={"depth": 8, "reliability": "best_effort", "durability": "volatile"},
                summary="Bounded real Core Service/Action metadata, original identifiers and explicit loss counters.",
                side_effects="read_only", contract_version="1.0.0")
        except BaseException:
            self.close()
            raise

    def publish(self, batch):
        self.publisher.publish(batch)

    def close(self):
        try:
            if self.node is not None:
                self.node.destroy_node()
        finally:
            if self.context.ok():
                self.context.shutdown()


class RequestTelemetryExporter:
    """One destructive drain owner, bounded pending data, independent publication.

    The private factory is solely for offline adapter tests. It does not stand in
    for a real Core-to-Topic acceptance run. No business operation is retried.
    """
    def __init__(self, source_context, *, topic, name, namespace, context_options=None,
                 interface_directory=None, known_executors=(), capture_capacity=256,
                 batch_records=MAX_BATCH_RECORDS, max_batch_bytes=MAX_BATCH_BYTES,
                 pending_records=512, pending_bytes=256 * 1024, poll_interval=0.05,
                 _transport_factory=None):
        for label, value, maximum in (("capture_capacity", capture_capacity, 4096),
                ("batch_records", batch_records, MAX_BATCH_RECORDS),
                ("max_batch_bytes", max_batch_bytes, MAX_BATCH_BYTES),
                ("pending_records", pending_records, 4096), ("pending_bytes", pending_bytes, 1024 * 1024)):
            if not _uint(value, maximum) or value < 1:
                raise ValueError(f"invalid {label}")
        if max_batch_bytes < 4096 or not isinstance(poll_interval, (int, float)) or not math.isfinite(poll_interval) or not 0.01 <= poll_interval <= 1:
            raise ValueError("batch bytes must be at least 4096; poll interval must be in [0.01, 1]")
        if not _matches(topic, r"/(?:[A-Za-z_][A-Za-z0-9_]*)(?:/[A-Za-z_][A-Za-z0-9_]*)*", 512):
            raise ValueError("telemetry topic must be an absolute non-root Topic")
        check_executors(known_executors)
        self.source = source_context
        initial = source_context.telemetry_snapshot()
        self.context_id, self.process_id = initial["context_id"], initial["process_id"]
        self.stream_id = uuid.uuid4().hex
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        self._transport = None
        self._queue = deque()
        self._queue_bytes = 0
        self._batch_sequence = 0
        self._stats = dict.fromkeys(EXPORT_COUNTERS, 0)
        self._before = self._after = _source_status(initial)
        self._worker_failed = False
        self._cleanup_complete = False
        self._batch_records, self._max_batch_bytes = batch_records, max_batch_bytes
        self._pending_records, self._pending_bytes = pending_records, pending_bytes
        self._poll_interval = poll_interval
        with _OWNER_LOCK:
            if self.context_id in _OWNERS or initial["log_capture_enabled"] or initial["log_queued"]:
                raise ValueError("source Context already has a capture/drain owner or undrained records")
            _OWNERS.add(self.context_id)
        capture_started = False
        try:
            factory = _TopicTransport if _transport_factory is None else _transport_factory
            self._transport = factory(topic=topic, name=name, namespace=namespace,
                context_options=dict(context_options or {}), interface_directory=interface_directory)
            target = self._transport.context.telemetry_snapshot()
            if target["context_id"] == self.context_id or target["log_capture_enabled"]:
                raise ValueError("export Context must be independent with capture disabled")
            self.source.enable_log_capture(capture_capacity)
            capture_started = True
            self._thread = threading.Thread(target=self._run, name="request-telemetry-exporter", daemon=True)
            self._thread.start()
        except BaseException:
            if capture_started:
                self.source.disable_log_capture()
            try:
                if self._transport is not None:
                    self._transport.close()
            finally:
                with _OWNER_LOCK:
                    _OWNERS.discard(self.context_id)
            raise

    def _increment(self, name, amount=1):
        with self._lock:
            self._stats[name] = min(UINT64, self._stats[name] + amount)

    def _capture(self):
        before = self.source.telemetry_snapshot()
        if not before["log_capture_enabled"] and not self._stop.is_set():
            raise RuntimeError("source capture was disabled by another owner")
        records = self.source.drain_log_records()
        after = self.source.telemetry_snapshot()
        with self._lock:
            self._before, self._after = _source_status(before), _source_status(after)
        self._increment("drained_records_total", len(records))
        for record in records:
            if isinstance(record, dict) and record.get("component") in (
                    "context", "node", "topic", "parameter", "lifecycle", "graph", "clock", "transport"):
                self._increment("filtered_records_total")
                continue
            try:
                validate_record(record)
                if record["context_id"] != self.context_id or record["process_id"] != self.process_id:
                    raise ValueError("unexpected source")
                size = len(encoded(record))
            except (ValueError, TypeError, UnicodeError):
                self._increment("invalid_records_total")
                continue
            with self._lock:
                overflow = len(self._queue) >= self._pending_records or self._queue_bytes + size > self._pending_bytes
                if not overflow:
                    self._queue.append((record, size))
                    self._queue_bytes += size
            if overflow:
                self._increment("queue_dropped_records_total")
                self._increment("queue_dropped_bytes_total", size)

    def _envelope(self, records):
        with self._lock:
            return {"schema_version": 1, "stream_id": self.stream_id, "batch_sequence": self._batch_sequence,
                "source_context_id": self.context_id, "source_process_id": self.process_id,
                "source_before": dict(self._before), "source_after": dict(self._after),
                "statistics": dict(self._stats), "records": records}

    def _publish(self, *, heartbeat=False):
        first = True
        while self._queue or heartbeat and first:
            first = False
            target = self._transport.context.telemetry_snapshot()
            if target["log_capture_enabled"]:
                self._increment("export_capture_errors_total")
                raise RuntimeError("export Context capture was enabled")
            self._batch_sequence += 1
            self._increment("publish_attempts_total")
            records = []
            while self._queue and len(records) < self._batch_records:
                record, size = self._queue[0]
                if len(encoded(self._envelope(records + [record]))) > self._max_batch_bytes:
                    if records:
                        break
                    with self._lock:
                        self._queue.popleft()
                        self._queue_bytes -= size
                    self._increment("oversized_records_total")
                    continue
                with self._lock:
                    self._queue.popleft()
                    self._queue_bytes -= size
                records.append(record)
            batch = self._envelope(records)
            validate_batch(batch)
            try:
                self._transport.publish(batch)
            except Exception:
                self._increment("publish_errors_total")
                self._increment("publish_failed_records_total", len(records))
            else:
                self._increment("publish_submitted_total")

    def _run(self):
        try:
            heartbeat_at = 0.0
            while not self._stop.is_set():
                self._capture()
                now = time.monotonic()
                heartbeat = now >= heartbeat_at
                self._publish(heartbeat=heartbeat)
                if heartbeat:
                    heartbeat_at = now + 1.0
                self._stop.wait(self._poll_interval)
            self._capture()
            self._publish(heartbeat=True)
        except Exception:
            self._increment("source_errors_total")
            with self._lock:
                self._worker_failed = True
        finally:
            try:
                self.source.disable_log_capture()
                self._transport.close()
                with self._lock:
                    self._cleanup_complete = True
            except Exception:
                with self._lock:
                    self._worker_failed = True
            finally:
                with _OWNER_LOCK:
                    _OWNERS.discard(self.context_id)

    def snapshot(self):
        try:
            current = _source_status(self.source.telemetry_snapshot())
        except Exception:
            current = None
        with self._lock:
            return {"stream_id": self.stream_id, "source_context_id": self.context_id,
                "statistics": dict(self._stats), "source_before": dict(self._before), "source_after": dict(self._after),
                "source_current": current, "source_current_read_failed": current is None,
                "pending_records": len(self._queue), "pending_bytes": self._queue_bytes,
                "worker_failed": self._worker_failed, "cleanup_complete": self._cleanup_complete,
                "worker_stopped": self._thread is not None and not self._thread.is_alive(),
                "end_to_end_delivery": "unknown"}

    def close(self, timeout=2.0):
        if not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or not 0 <= timeout <= 30:
            raise ValueError("close timeout must be finite and in [0, 30]")
        self._stop.set()
        self.source.disable_log_capture()
        if self._thread is not None:
            self._thread.join(timeout)
        return self.snapshot()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        # Diagnostics failures are reported without changing a business result.
        print(json.dumps({"event": "request_telemetry_exporter_stopped", **self.close()}), flush=True)


def exporter_from_args(args, context, context_options, *, known_executors=()):
    if args.telemetry_topic is None:
        return nullcontext(None)
    return RequestTelemetryExporter(context, topic=args.telemetry_topic,
        name=args.name + "_request_exporter", namespace=args.namespace,
        context_options=context_options, interface_directory=args.interfaces, known_executors=known_executors)


def observe(subscription, *, count, timeout, max_output_bytes, timeout_error, output):
    """One parse/output path for live and replay Topics; no command interpretation."""
    seen = {}
    summary = {"batches": 0, "records": 0, "duplicate_or_reordered_batches": 0,
               "observed_batch_gaps": 0, "output_bytes": 0, "reason": "timeout"}
    deadline = time.monotonic() + timeout
    while summary["batches"] < count and time.monotonic() < deadline:
        try:
            batch = subscription.recv(min(0.2, max(0.001, deadline - time.monotonic())))
        except timeout_error:
            continue
        validate_batch(batch)
        stream, sequence = batch["stream_id"], batch["batch_sequence"]
        if stream not in seen and len(seen) >= 64:
            summary["reason"] = "stream_limit"
            break
        line = encoded({"event": "request_telemetry_batch", "batch": batch}) + b"\n"
        if summary["output_bytes"] + len(line) > max_output_bytes:
            summary["reason"] = "output_limit"
            break
        previous = seen.get(stream)
        if previous is not None:
            if sequence <= previous:
                summary["duplicate_or_reordered_batches"] += 1
            else:
                summary["observed_batch_gaps"] += sequence - previous - 1
        seen[stream] = max(previous or 0, sequence)
        output(line)
        summary["output_bytes"] += len(line)
        summary["batches"] += 1
        summary["records"] += len(batch["records"])
    if summary["batches"] == count:
        summary["reason"] = "count"
    summary["streams"] = len(seen)
    summary["before_first_batch_delivery"] = "unknown"
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--config", type=Path)
    selection.add_argument("--security-profile", type=Path)
    parser.add_argument("--domain-id", type=int)
    parser.add_argument("--namespace", default="/memory_observation")
    parser.add_argument("--name", default="request_observer")
    parser.add_argument("--interfaces", type=Path)
    parser.add_argument("--topic", required=True, help="Explicit live or isolated replay Topic; never a business endpoint.")
    parser.add_argument("--count", type=int, default=10, help="Stop after 1..4096 batches, including heartbeat batches.")
    parser.add_argument("--timeout", type=float, default=10)
    parser.add_argument("--max-output-bytes", type=int, default=4 * 1024 * 1024)
    args = parser.parse_args(argv)
    if not 1 <= args.count <= 4096 or not math.isfinite(args.timeout) or not 0 < args.timeout <= 300:
        parser.error("count must be in 1..4096 and timeout in (0, 300]")
    if not 4096 <= args.max_output_bytes <= 16 * 1024 * 1024:
        parser.error("max-output-bytes must be in 4096..16777216")
    if not _matches(args.topic, r"/(?:[A-Za-z_][A-Za-z0-9_]*)(?:/[A-Za-z_][A-Za-z0-9_]*)*", 512):
        parser.error("topic must be absolute and non-root")
    import aos_sdk as aos
    options = {key: getattr(args, key) for key in ("config", "security_profile", "domain_id")
               if getattr(args, key) is not None}
    with aos.Context(**options) as context:
        with aos.Node(args.name, args.namespace, context, start_parameter_services=False,
                      summary="Only observes request diagnostic Messages; never calls the recorded endpoints.",
                      tags=("diagnostics", "observer")) as node:
            subscription = node.create_subscription(_interface(aos, args.interfaces), args.topic,
                qos_profile={"depth": 8, "reliability": "best_effort", "durability": "volatile"},
                side_effects="read_only", contract_version="1.0.0")
            summary = observe(subscription, count=args.count, timeout=args.timeout,
                max_output_bytes=args.max_output_bytes, timeout_error=aos.WaitTimeoutError,
                output=lambda line: (sys.stdout.buffer.write(line), sys.stdout.buffer.flush()))
    print(json.dumps({"event": "request_observer_stopped", **summary}), flush=True)
    return 0 if summary["reason"] == "count" else 1


if __name__ == "__main__":
    raise SystemExit(main())
