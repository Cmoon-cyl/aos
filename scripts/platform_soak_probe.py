#!/usr/bin/env python3
"""Run owned AOS mixed-load probes; smoke success never means 24-hour acceptance.

Use an installed-wheel Python with -I. This script does not build, install,
upload, or resume candidates. It owns only the new output directory and the
child processes it starts. Formal runs require a separately frozen workload,
artifact hashes, numerical thresholds, and baseline evidence.
"""

from __future__ import annotations

import argparse
from collections import Counter
import contextlib
import hashlib
import importlib.machinery
import json
import math
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import sysconfig
import tempfile
import threading
import time
import uuid
import zipfile


SCRIPT = Path(__file__).resolve()
ROOT = SCRIPT.parent.parent
PROTOCOL = "aos.platform.soak.v1"
DAY = 24 * 60 * 60
HASH = re.compile(r"[0-9a-f]{64}\Z")
SHA = re.compile(r"[0-9a-f]{40}\Z")
CASES = ("small_topic", "small_topic_ceiling", "large_topic", "service", "action", "mixed", "mixed_ceiling")
TRANSPORTS = ("tcp", "mtls")
RAW_MANIFEST = "raw-evidence-manifest.json"
# result.json binds this manifest. A baseline aggregate (then the freeze) binds
# result.json, so neither the manifest nor its owning summary hashes itself.
RAW_EXCLUSIONS = (RAW_MANIFEST, "result.json")
INFLIGHT_SCOPES = {
    "client": {"service_calls": "service", "action_requests": "action"},
    "provider": {"service_callbacks": "service", "action_executions": "action"},
}


def active(config, component):
    case = config.get("workload_case", "mixed")
    return case.startswith("mixed") or case.removesuffix("_ceiling") == component


def transport(config):
    return config.get("transport", "tcp")


def configured_client_threads(config):
    return {name: config["workload"][name]["concurrency"] if active(config, name) else 0
            for name in ("service", "action")}


class ProbeError(RuntimeError):
    def __init__(self, code, detail):
        self.code = code
        super().__init__(detail)


def require(value, code, detail):
    if not value:
        raise ProbeError(code, detail)


def digest(path):
    checksum = hashlib.sha256()
    with Path(path).open("rb") as stream:
        while block := stream.read(1024 * 1024):
            checksum.update(block)
    return checksum.hexdigest()


def write_json(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, allow_nan=False)
        stream.write("\n")
    temporary.replace(path)


def load_json(path):
    with Path(path).open(encoding="utf-8") as stream:
        return json.load(stream)


def raw_evidence_files(folder):
    """Inventory regular evidence files without following links outside a run."""
    folder = Path(folder).resolve(strict=True)
    files = {}
    for path in sorted(folder.rglob("*")):
        require(not path.is_symlink(), "RAW_EVIDENCE_PATH", "evidence cannot contain symlinks")
        if path.is_dir():
            continue
        require(path.is_file(), "RAW_EVIDENCE_PATH", "evidence must contain only regular files")
        relative = path.relative_to(folder).as_posix()
        if relative not in RAW_EXCLUSIONS:
            files[relative] = path
    return files


def seal_raw_evidence(folder, run_id):
    """Called only after owned processes, log readers and resource sampling stop."""
    folder = Path(folder)
    files = raw_evidence_files(folder)
    manifest = {"schema_version": 1, "protocol": PROTOCOL, "run_id": run_id,
                "excluded": list(RAW_EXCLUSIONS),
                "boundary": "raw files -> this manifest -> result.json -> baseline-result.json -> freeze; no self-hash",
                "files": [{"path": name, "bytes": path.stat().st_size, "sha256": digest(path)}
                          for name, path in files.items()]}
    write_json(folder / RAW_MANIFEST, manifest)
    return {"path": RAW_MANIFEST, "sha256": digest(folder / RAW_MANIFEST)}


def verify_raw_evidence(folder, result):
    folder = Path(folder).resolve(strict=True)
    reference = result.get("raw_evidence", {})
    require(isinstance(reference, dict) and set(reference) == {"path", "sha256"} and reference["path"] == RAW_MANIFEST and
            HASH.fullmatch(str(reference["sha256"])), "RAW_EVIDENCE_MISSING", "result must bind its raw evidence manifest")
    path = folder / RAW_MANIFEST
    require(not path.is_symlink() and path.is_file(), "RAW_EVIDENCE_PATH", "missing or linked raw manifest")
    require(digest(path) == reference["sha256"], "RAW_EVIDENCE_CHANGED", "raw manifest changed")
    manifest = load_json(path)
    require(manifest.get("schema_version") == 1 and manifest.get("protocol") == PROTOCOL and
            manifest.get("run_id") == result.get("run_id") and manifest.get("excluded") == list(RAW_EXCLUSIONS),
            "RAW_EVIDENCE_INVALID", "raw manifest identity or exclusions differ")
    listed = {}
    require(isinstance(manifest.get("files"), list), "RAW_EVIDENCE_INVALID", "raw files must be listed")
    for item in manifest["files"]:
        require(isinstance(item, dict) and set(item) == {"path", "bytes", "sha256"},
                "RAW_EVIDENCE_INVALID", "invalid raw file entry")
        name = item["path"]
        relative = Path(name) if isinstance(name, str) else Path("..")
        require(name and not relative.is_absolute() and ".." not in relative.parts and
                relative.as_posix() == name and name not in RAW_EXCLUSIONS and name not in listed,
                "RAW_EVIDENCE_PATH", "raw paths must be unique and relative to the run")
        require(type(item["bytes"]) is int and item["bytes"] >= 0 and HASH.fullmatch(str(item["sha256"])),
                "RAW_EVIDENCE_INVALID", "invalid raw file size or digest")
        listed[name] = item
    actual = raw_evidence_files(folder)
    require(set(actual) == set(listed) and "run.json" in listed,
            "RAW_EVIDENCE_INVENTORY", "raw evidence files were added, removed or omitted")
    for name, item in listed.items():
        require(actual[name].stat().st_size == item["bytes"] and digest(actual[name]) == item["sha256"],
                "RAW_EVIDENCE_CHANGED", "raw evidence changed: " + name)
    # The summary does not hash itself. Bind its identity and copied worker data
    # back to raw files; higher-level indexes pin the summary's own bytes.
    initial = load_json(folder / "run.json")
    for key in ("protocol", "source_sha", "binding", "harness_source", "harness_sha256", "run_id", "namespace",
                "config", "input_config_sha256", "platform", "hardware", "scope", "configured_client_threads"):
        require(result.get(key) == initial.get(key), "RAW_SUMMARY_MISMATCH", "run identity differs: " + key)
    for role in ("provider", "client"):
        name = role + "-final.json"
        if name in listed:
            require(result.get("workers", {}).get(role) == load_json(folder / name),
                    "RAW_SUMMARY_MISMATCH", "worker summary differs: " + role)
    if "bag-final.json" in listed:
        require(result.get("bag") == load_json(folder / "bag-final.json"),
                "RAW_SUMMARY_MISMATCH", "bag summary differs")
    require(digest(path) == reference["sha256"] and set(raw_evidence_files(folder)) == set(listed),
            "RAW_EVIDENCE_CHANGED", "evidence changed while being verified")


def load_verified_run(path):
    path = Path(path)
    result = load_json(path)
    verify_raw_evidence(path.parent, result)
    if result["config"]["mode"] == "soak":
        verified = verify_freeze(result["config"], result["binding"], scope=result.get("scope"),
                                 harness_sha256=result.get("harness_sha256"))
        require(verified == result.get("freeze_verification") == result.get("freeze_after"),
                "FREEZE_EVIDENCE_CHANGED", "saved matrix verification differs")
    return result


def number(value, name, lower=0, upper=math.inf, *, integer=False):
    require(type(value) in ((int,) if integer else (int, float)),
            "INVALID_CONFIG", f"{name} must be a finite number")
    require(math.isfinite(value) and lower <= value <= upper,
            "INVALID_CONFIG", f"{name} must be in [{lower}, {upper}]")
    return value


def local_scope_identity():
    return {"kind": "local", "system": platform.system(), "machine": platform.machine(),
            "hostname": socket.gethostname()}


def round_scope(result):
    hardware = result.get("hardware", {})
    require(all(isinstance(hardware.get(key), str) and hardware[key] for key in ("system", "machine", "hostname")),
            "BASELINE_SCOPE_MISSING", "baseline must identify its actual OS, architecture and host")
    scope = {"kind": "local", **{key: hardware[key] for key in ("system", "machine", "hostname")}}
    require(result.get("scope") == scope, "BASELINE_SCOPE_MISMATCH", "local evidence cannot be relabelled as another topology")
    return scope


def validate_freeze_matrix(freeze):
    matrix = freeze.get("baseline_matrix")
    require(isinstance(matrix, dict) and set(matrix) == {"schema_version", "local", "cross_machine"} and
            matrix["schema_version"] == 1 and isinstance(matrix["local"], list) and matrix["local"] and
            isinstance(matrix["cross_machine"], list),
            "UNFROZEN_MATRIX", "declare local and cross-machine baseline scopes separately")
    require(HASH.fullmatch(str(freeze.get("harness_sha256", ""))),
            "UNFROZEN_HARNESS", "freeze the harness digest independently of the native source SHA")
    scopes, identities = {}, set()
    for item in matrix["local"]:
        require(isinstance(item, dict) and set(item) == {"id", "system", "machine", "hostname", "artifact_sha256"},
                "INVALID_MATRIX", "invalid local scope")
        require(all(isinstance(item[key], str) and item[key] for key in ("id", "system", "machine", "hostname")) and
                item["system"] in ("Darwin", "Linux"), "INVALID_MATRIX", "local scope needs an actual Mac or Linux host identity")
        identity = tuple(item[key] for key in ("system", "machine", "hostname"))
        require(item["id"] not in scopes and identity not in identities,
                "INVALID_MATRIX", "local scope ids and host identities must be unique")
        artifacts = item["artifact_sha256"]
        require(isinstance(artifacts, dict) and set(artifacts) == {"wheel", "cli", "router"} and
                all(HASH.fullmatch(str(value)) for value in artifacts.values()),
                "INVALID_MATRIX", "each platform needs its own wheel, CLI and router digests")
        scopes[item["id"]] = item
        identities.add(identity)
    ids = set(scopes)
    for item in matrix["cross_machine"]:
        require(isinstance(item, dict) and set(item) == {"id", "provider_id", "client_id"} and
                isinstance(item["id"], str) and item["id"] and item["id"] not in ids and
                item["provider_id"] in scopes and item["client_id"] in scopes and item["provider_id"] != item["client_id"],
                "INVALID_MATRIX", "cross-machine scopes must identify distinct provider and client hosts")
        ids.add(item["id"])
    return scopes


def validate_config(config):
    allowed = {
        "schema_version", "mode", "duration_seconds", "warmup_seconds", "sample_seconds",
        "maximum_sample_gap_seconds", "operation_timeout_seconds", "startup_timeout_seconds",
        "shutdown_timeout_seconds", "quiet_seconds", "domain_id", "workload", "faults",
        "storage", "thresholds", "freeze",
    }
    optional = {"transport", "workload_case", "rounds"}
    require(allowed <= set(config) <= allowed | optional, "INVALID_CONFIG", "missing or unknown top-level config field")
    require(config["schema_version"] == 1, "INVALID_CONFIG", "unsupported config version")
    require(config["mode"] in ("smoke", "baseline", "soak"), "INVALID_CONFIG", "invalid mode")
    require(transport(config) in TRANSPORTS, "INVALID_CONFIG", "transport must be tcp or mtls")
    require(config.get("workload_case", "mixed") in CASES, "INVALID_CONFIG", "unsupported workload case")
    number(config.get("rounds", 1), "rounds", 1, 10, integer=True)
    require(config["mode"] == "baseline" or config.get("rounds", 1) == 1,
            "INVALID_CONFIG", "only baseline mode coordinates multiple rounds")
    for name in ("duration_seconds", "sample_seconds", "maximum_sample_gap_seconds",
                 "operation_timeout_seconds", "startup_timeout_seconds", "shutdown_timeout_seconds"):
        number(config[name], name, .01, 7 * DAY)
    for name in ("warmup_seconds", "quiet_seconds"):
        number(config[name], name, 0, DAY)
    number(config["domain_id"], "domain_id", 0, 2**32 - 1, integer=True)
    require(config["maximum_sample_gap_seconds"] >= config["sample_seconds"],
            "INVALID_CONFIG", "maximum sample gap must cover the sampling interval")
    work = config["workload"]
    require(set(work) == {"small_topic", "large_topic", "service", "action", "churn", "management", "bag"},
            "INVALID_CONFIG", "workload must specify every mixed-load component")
    for name in ("small_topic", "large_topic", "service"):
        number(work[name]["payload_bytes"], name + ".payload_bytes", 512, 16 * 1024**2, integer=True)
        number(work[name]["hz"], name + ".hz", .01, 100_000)
    small = work["small_topic"]
    number(small["fast_consumers"], "fast_consumers", 1, 16, integer=True)
    number(small["slow_consumers"], "slow_consumers", 1, 16, integer=True)
    number(small["slow_delay_seconds"], "slow_delay_seconds", .001, 10)
    for item in (small, work["large_topic"]):
        number(item["depth"], "topic depth", 1, 4096, integer=True)
    number(work["service"]["concurrency"], "service concurrency", 1, 64, integer=True)
    action = work["action"]
    number(action["concurrency"], "action concurrency", 1, 32, integer=True)
    number(action["duration_seconds"], "action duration", .1, 3600)
    number(action["cancel_duration_seconds"], "cancel duration", .1, 3600)
    number(action["feedback_hz"], "feedback_hz", 1, 1000)
    number(action["cancel_every"], "cancel_every", 2, 100, integer=True)
    require(config["shutdown_timeout_seconds"] >= max(action["duration_seconds"], action["cancel_duration_seconds"]) +
            config["operation_timeout_seconds"] + 2,
            "INVALID_CONFIG", "shutdown budget must allow in-flight Actions to finish")
    number(work["churn"]["rounds"], "churn rounds", 1, 100_000, integer=True)
    number(work["churn"]["interval_seconds"], "churn interval", .05, DAY)
    number(work["management"]["interval_seconds"], "management interval", .1, DAY)
    require(work["bag"]["enabled"] is True, "INVALID_CONFIG", "the mixed load includes recording")
    for name in ("total_bytes", "queue_messages", "queue_bytes", "adapter_queue_bytes", "message_bytes"):
        number(work["bag"][name], "bag." + name, 1, 1024**4, integer=True)
    for name in ("logs_bytes_per_process", "resource_log_bytes", "worker_log_bytes", "maximum_disk_bytes"):
        number(config["storage"][name], name, 4096, 1024**4, integer=True)
    require(work["bag"]["total_bytes"] < config["storage"]["maximum_disk_bytes"],
            "INVALID_CONFIG", "disk budget must leave room for evidence beside the bag")
    require(isinstance(config["faults"], list) and len(config["faults"]) <= 256 and
            (config["mode"] != "soak" or config["faults"]),
            "INVALID_CONFIG", "soak requires 1 to 256 isolated router restarts")
    previous_end = 0
    for fault in config["faults"]:
        require(set(fault) == {"kind", "at_seconds", "down_seconds", "recovery_timeout_seconds"}
                and fault["kind"] == "router_restart", "INVALID_CONFIG", "unsupported fault")
        for name in ("at_seconds", "down_seconds", "recovery_timeout_seconds"):
            number(fault[name], "fault." + name, .01, config["duration_seconds"])
        end = fault["at_seconds"] + fault["down_seconds"] + fault["recovery_timeout_seconds"]
        require(fault["at_seconds"] > previous_end and end < config["duration_seconds"],
                "INVALID_CONFIG", "fault windows overlap or leave no post-recovery load")
        previous_end = end
    thresholds = config["thresholds"]
    required = {
        "rss_bytes_per_process", "fd_per_process", "threads_per_process",
        "rss_growth_bytes_per_hour", "fd_growth_per_hour", "threads_growth_per_hour",
        "service_latency_p99_seconds", "action_accept_p99_seconds", "action_cancel_p99_seconds",
        "fast_topic_loss_fraction", "large_topic_loss_fraction", "maximum_business_silence_seconds",
        "require_queue_byte_stats",
    }
    require(set(thresholds) == required, "INVALID_CONFIG", "missing or unknown threshold")
    require(type(thresholds["require_queue_byte_stats"]) is bool,
            "INVALID_CONFIG", "require_queue_byte_stats must be boolean")
    for name, value in thresholds.items():
        if name != "require_queue_byte_stats" and value is not None:
            number(value, name, 0)
    if config["mode"] == "soak":
        require(config.get("workload_case", "mixed").startswith("mixed"), "INVALID_CONFIG", "24h acceptance uses the complete mixed workload")
        require(config["duration_seconds"] >= DAY, "DURATION_TOO_SHORT", "formal duration is at least 24h")
        require(all(value is not None for value in thresholds.values()),
                "UNFROZEN_THRESHOLD", "formal thresholds must be frozen before the run")
        require(thresholds["require_queue_byte_stats"], "UNFROZEN_THRESHOLD",
                "formal queue byte bounds cannot be unknown")
        freeze = config["freeze"]
        require(isinstance(freeze, dict) and freeze.get("id") and
                SHA.fullmatch(str(freeze.get("source_sha", ""))) and
                all(HASH.fullmatch(str(freeze.get("artifact_sha256", {}).get(name, "")))
                    for name in ("wheel", "cli", "router")) and freeze.get("baseline_evidence"),
                "UNFROZEN_CANDIDATE", "formal run requires candidate hashes and baseline evidence")
        validate_freeze_matrix(freeze)
    elif config["mode"] == "baseline":
        require(config["warmup_seconds"] >= 300 and config["duration_seconds"] >= 900 and
                config.get("rounds", 0) >= 3, "BASELINE_DURATION", "baseline requires at least 3 rounds of 300s warmup and 900s measurement")
        require(not config["faults"], "BASELINE_FAULTS", "measure a fault-free baseline; restart probes are separate")
        expected_hz = 1000 if config.get("workload_case", "mixed").endswith("_ceiling") else 100
        require(small["payload_bytes"] == 1024 and small["hz"] == expected_hz and
                small["fast_consumers"] == 2 and small["slow_consumers"] == 1 and
                work["large_topic"]["payload_bytes"] == 256 * 1024 and work["large_topic"]["hz"] == 10 and
                work["service"]["payload_bytes"] == 1024 and work["service"]["hz"] == 20 and
                work["service"]["concurrency"] == 4 and action["concurrency"] == 8 and
                action["duration_seconds"] == action["cancel_duration_seconds"] == 30 and
                action["feedback_hz"] == 10 and action["cancel_every"] == 4 and work["churn"]["rounds"] == 100,
                "BASELINE_WORKLOAD", "baseline workload must match acceptance.md; reduced loads remain smoke evidence")
        require(work["churn"]["interval_seconds"] * work["churn"]["rounds"] < config["duration_seconds"],
                "BASELINE_WORKLOAD", "all churn rounds must fit inside measurement")
    else:
        require(config["duration_seconds"] < DAY, "SMOKE_DURATION", "smoke is explicitly shorter than 24h")
    return config


def verify_artifacts(proof_path, source_sha, cli, router, *, package=None, native=None):
    """Verify actual installed package bytes, not a caller's build label alone."""
    proof_path = Path(proof_path).resolve(strict=True)
    proof = load_json(proof_path)
    require(SHA.fullmatch(source_sha), "SOURCE_MISMATCH", "expected full Git SHA")
    require(proof.get("status") == "passed" and proof.get("tracked_changes") == "",
            "BUILD_NOT_CLEAN", "native build must have passed from a clean tracked tree")
    require(proof.get("source_sha") == source_sha, "SOURCE_MISMATCH", "build source SHA differs")
    require(proof.get("final_source_sha") == source_sha and proof.get("final_source_status") == "",
            "BUILD_NOT_CLEAN", "build must verify the final source SHA and clean tree")
    require(proof.get("commands") and all(item.get("returncode") == 0 for item in proof["commands"]),
            "BUILD_NOT_PASSED", "build commands contain a failure or are missing")
    if package is None:
        import aos_sdk as aos
        from aos_sdk import _native
        require(sys.flags.isolated, "NOT_ISOLATED", "run with the installed Python and -I")
        package, native = Path(aos.__file__).resolve(), Path(_native.__file__).resolve()
        site = Path(sysconfig.get_paths()["platlib"]).resolve() / "aos_sdk"
        require(native.is_relative_to(site) and package.parent == native.parent and
                isinstance(_native.__loader__, importlib.machinery.ExtensionFileLoader),
                "NOT_INSTALLED_NATIVE", "SDK must be the installed compiled wheel")
    cli, router, native, package = map(lambda value: Path(value).resolve(strict=True),
                                     (cli, router, native, package))
    wheel = (proof_path.parent / proof["wheel"]["path"]).resolve(strict=True)
    hashes = {"wheel": digest(wheel), "cli": digest(cli), "router": digest(router)}
    expected = {"wheel": proof["wheel"]["sha256"], "cli": proof["cli"]["sha256"],
                "router": proof["zenohd"].get("executable_sha256", proof["zenohd"].get("sha256"))}
    require(hashes == expected, "ARTIFACT_MISMATCH", "wheel, CLI or router differs from build proof")
    installed = {}
    with zipfile.ZipFile(wheel) as archive:
        for member in archive.namelist():
            if not member.startswith("aos_sdk/") or member.endswith("/"):
                continue
            relative = Path(member)
            require(".." not in relative.parts and not relative.is_absolute(),
                    "INVALID_WHEEL", "unsafe wheel member")
            actual = package.parent.parent / relative
            checksum = hashlib.sha256(archive.read(member)).hexdigest()
            require(digest(actual) == checksum, "INSTALLED_MISMATCH", f"installed file differs: {member}")
            installed[member] = checksum
    require("aos_sdk/" + native.name in installed, "INSTALLED_MISMATCH", "native is absent from wheel")
    return {
        "source_sha": source_sha, "artifact_sha256": hashes,
        "native_sha256": digest(native), "installed_files": installed,
        "paths": {"proof": str(proof_path), "wheel": str(wheel), "cli": str(cli),
                  "router": str(router), "package": str(package), "native": str(native)},
        "proof_sha256": digest(proof_path),
        "method": "clean native build provenance plus byte comparison of every wheel aos_sdk member",
    }


def verify_freeze(config, binding, *, scope=None, harness_sha256=None):
    if config["mode"] != "soak":
        return
    freeze = config["freeze"]
    scopes = validate_freeze_matrix(freeze)
    require(not freeze["baseline_matrix"]["cross_machine"], "CROSS_MACHINE_BASELINE_UNSUPPORTED",
            "this local harness cannot validate cross-machine evidence; that matrix needs a separate producer and verifier")
    scope = local_scope_identity() if scope is None else scope
    harness_sha256 = digest(SCRIPT) if harness_sha256 is None else harness_sha256
    require(harness_sha256 == freeze["harness_sha256"], "HARNESS_MISMATCH", "frozen harness differs")
    matching = [item for item in scopes.values() if scope ==
                {"kind": "local", **{key: item[key] for key in ("system", "machine", "hostname")}}]
    require(len(matching) == 1, "MATRIX_SCOPE_MISSING", "the actual local run is absent from the declared matrix")
    current_id = matching[0]["id"]
    require(freeze["source_sha"] == binding["source_sha"], "SOURCE_MISMATCH", "frozen source differs")
    require(freeze["artifact_sha256"] == binding["artifact_sha256"],
            "ARTIFACT_MISMATCH", "frozen artifacts differ")
    require(matching[0]["artifact_sha256"] == binding["artifact_sha256"],
            "ARTIFACT_MISMATCH", "current platform artifacts differ from its matrix row")
    expected = {(name, case, network) for name in scopes for case in CASES for network in TRANSPORTS}
    require(isinstance(freeze["baseline_evidence"], list) and len(freeze["baseline_evidence"]) == len(expected),
            "BASELINE_MATRIX_INCOMPLETE", "each declared local host requires 14 independent baseline records")
    observed = set()
    for evidence in freeze["baseline_evidence"]:
        require(isinstance(evidence, dict) and set(evidence) == {"scope_id", "path", "sha256"} and
                evidence["scope_id"] in scopes and isinstance(evidence["path"], str) and
                Path(evidence["path"]).is_absolute() and HASH.fullmatch(str(evidence["sha256"])),
                "INVALID_FREEZE", "baseline evidence must identify a declared scope, file and SHA256")
        require(digest(evidence["path"]) == evidence["sha256"],
                "BASELINE_MISMATCH", "baseline evidence changed")
        gate, _ = evaluate_baseline(Path(evidence["path"]))
        require(gate["baseline_passed"], "BASELINE_NOT_PASSED", "frozen evidence must contain complete baseline rounds")
        record = load_json(evidence["path"])
        target = scopes[evidence["scope_id"]]
        expected_scope = {"kind": "local", **{key: target[key] for key in ("system", "machine", "hostname")}}
        require(record.get("scope") == expected_scope, "BASELINE_SCOPE_MISMATCH",
                "baseline hardware/topology differs from its declared scope")
        candidate = record.get("binding", {})
        require(candidate.get("source_sha") == freeze["source_sha"] and
                candidate.get("artifact_sha256") == target["artifact_sha256"],
                "BASELINE_CANDIDATE_CHANGED", "baseline used another platform candidate")
        if evidence["scope_id"] == current_id:
            require(candidate == binding, "BASELINE_CANDIDATE_CHANGED", "local baseline used another installed candidate")
        require(record.get("harness_sha256") == freeze["harness_sha256"],
                "HARNESS_MISMATCH", "baseline used another harness")
        cell = (evidence["scope_id"], record["config"].get("workload_case", "mixed"), transport(record["config"]))
        require(cell not in observed, "BASELINE_MATRIX_DUPLICATE", "a matrix cell was supplied more than once")
        observed.add(cell)
    require(observed == expected, "BASELINE_MATRIX_INCOMPLETE",
            "every declared local host requires all 7 workload cases over TCP and mTLS")
    return {"id": freeze["id"], "scope": "declared local baseline matrix only",
            "verified_cells": [list(cell) for cell in sorted(observed)],
            "cross_machine_covered": False, "c20_acceptance_passed": False}


class BoundedJsonl:
    def __init__(self, path, maximum):
        self.path = Path(path)
        self.maximum = maximum
        self.written = 0
        self.lock = threading.Lock()
        self.stream = self.path.open("xb")

    def emit(self, value):
        data = (json.dumps(value, separators=(",", ":"), allow_nan=False) + "\n").encode()
        with self.lock:
            require(self.written + len(data) <= self.maximum, "EVIDENCE_CAPACITY", "evidence byte budget exhausted")
            self.stream.write(data)
            self.stream.flush()
            self.written += len(data)

    def close(self):
        with self.lock:
            self.stream.close()


class Trend:
    """Constant-memory least-squares growth estimate, in units per hour."""
    def __init__(self):
        self.n = 0
        self.x = self.y = self.xx = self.xy = 0.0
        self.first = self.last = self.maximum = None

    def add(self, seconds, value):
        if value is None:
            return
        self.n += 1
        self.x += seconds
        self.y += value
        self.xx += seconds * seconds
        self.xy += seconds * value
        self.first = value if self.first is None else self.first
        self.last = value
        self.maximum = value if self.maximum is None else max(self.maximum, value)

    def report(self):
        denominator = self.n * self.xx - self.x * self.x
        slope = (self.n * self.xy - self.x * self.y) / denominator * 3600 if denominator > 0 else None
        return {"samples": self.n, "first": self.first, "last": self.last,
                "maximum": self.maximum, "growth_per_hour": slope}


def parse_cpu_time(value):
    days = 0
    if "-" in value:
        left, value = value.split("-", 1)
        days = int(left)
    result = 0.0
    for field in value.split(":"):
        result = result * 60 + float(field)
    return result + days * DAY


def process_resources(pid):
    """Read only the explicitly owned PID. Unavailable metrics remain null."""
    result = {"pid": pid, "rss_bytes": None, "fds": None, "threads": None,
              "cpu_seconds": None, "unavailable": []}
    if sys.platform.startswith("linux"):
        folder = Path("/proc") / str(pid)
        try:
            status = dict(line.split(":", 1) for line in (folder / "status").read_text().splitlines())
            result["rss_bytes"] = int(status["VmRSS"].split()[0]) * 1024
            result["threads"] = int(status["Threads"].strip())
            result["fds"] = len(list((folder / "fd").iterdir()))
            fields = (folder / "stat").read_text().rpartition(") ")[2].split()
            result["cpu_seconds"] = (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")
            result["start_ticks"] = int(fields[19])
        except (OSError, KeyError, ValueError, IndexError) as error:
            result["unavailable"].append(type(error).__name__)
    elif sys.platform == "darwin":
        commands = {
            "process": ["ps", "-p", str(pid), "-o", "pid=,rss=,time="],
            "threads": ["ps", "-M", "-p", str(pid)],
            "fds": ["lsof", "-a", "-p", str(pid), "-F", "f"],
        }
        for name, command in commands.items():
            try:
                output = subprocess.run(command, capture_output=True, text=True, timeout=2, check=True).stdout
                if name == "process":
                    actual, rss, cpu = output.split()
                    require(int(actual) == pid, "RESOURCE_PID", "resource sample returned another PID")
                    result.update(rss_bytes=int(rss) * 1024, cpu_seconds=parse_cpu_time(cpu))
                elif name == "threads":
                    lines = [line for line in output.splitlines() if line.strip()]
                    require(lines and "PID" in lines[0], "RESOURCE_PARSE", "unexpected ps -M header")
                    result["threads"] = len(lines) - 1
                else:
                    result["fds"] = sum(bool(re.fullmatch(r"f[0-9]+", line)) for line in output.splitlines())
            except (OSError, ValueError, subprocess.SubprocessError, ProbeError) as error:
                result["unavailable"].append(name + ":" + type(error).__name__)
    else:
        result["unavailable"].append("unsupported_platform")
    return result


def queue_violations(name, stats, require_bytes=False):
    violations = []
    if not isinstance(stats, dict):
        return [name + ":queue_stats_unavailable"]
    if not (0 <= stats.get("queued", -1) <= stats.get("depth", -1)):
        violations.append(name + ":queue_count_bound")
    byte_stats = stats.get("bytes")
    if byte_stats is None:
        if require_bytes:
            violations.append(name + ":queue_bytes_unavailable")
    elif not (0 <= byte_stats.get("queued_bytes", -1) + byte_stats.get("reserved_bytes", -1)
              <= byte_stats.get("queue_limit_bytes", -1)):
        violations.append(name + ":queue_byte_bound")
    return violations


class OwnedProcess:
    def __init__(self, name, command, output, maximum_log_bytes, *, environment=None):
        self.name = name
        self.command = [str(item) for item in command]
        self.log = Path(output) / (name + ".log")
        self.maximum_log_bytes = maximum_log_bytes
        self.log_bytes = 0
        self.discarded_log_bytes = 0
        self.expected_exit = False
        self.started_ns = time.monotonic_ns()
        self.process = subprocess.Popen(self.command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT, cwd=output, env=environment,
                                        start_new_session=os.name == "posix")
        self.reader = threading.Thread(target=self._copy_log, name=name + "-log", daemon=True)
        self.reader.start()

    def _copy_log(self):
        with self.process.stdout, self.log.open("xb") as stream:
            while data := self.process.stdout.read1(65536):
                left = max(0, self.maximum_log_bytes - self.log_bytes)
                kept = data[:left]
                stream.write(kept)
                stream.flush()
                self.log_bytes += len(kept)
                self.discarded_log_bytes += len(data) - len(kept)

    def stop(self, timeout=5, signum=signal.SIGTERM):
        self.expected_exit = True
        forced = False
        if self.process.poll() is None:
            if os.name == "nt":
                # Windows has no POSIX process-group signals. Stop only this
                # owned tree and report forced cleanup, never a graceful exit.
                forced = True
                subprocess.run(["taskkill", "/PID", str(self.process.pid), "/T", "/F"],
                               stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
                               timeout=timeout, check=False)
                self.process.wait(timeout=5)
            else:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(self.process.pid, signum)
                try:
                    self.process.wait(timeout)
                except subprocess.TimeoutExpired:
                    forced = True
                    with contextlib.suppress(ProcessLookupError):
                        os.killpg(self.process.pid, signal.SIGKILL)
                    self.process.wait(timeout=5)
        self.reader.join(3)
        return {"pid": self.process.pid, "returncode": self.process.returncode,
                "forced": forced, "reaped": self.process.poll() is not None,
                "log_complete": not self.reader.is_alive() and self.discarded_log_bytes == 0}


class ResourceSampler:
    def __init__(self, config, output, processes, phase):
        self.config = config
        self.processes = processes
        self.phase = phase
        self.log = BoundedJsonl(Path(output) / "resources.jsonl", config["storage"]["resource_log_bytes"])
        self.stop_event = threading.Event()
        self.failures = []
        self.trends = {}
        self.peaks = {}
        self.unavailable = Counter()
        self.previous_cpu = {}
        self.cpu = {}
        self.samples = 0
        self.started = time.monotonic()
        self.thread = threading.Thread(target=self.run, name="owned-resource-sampler", daemon=True)

    def run(self):
        previous_mono = previous_wall = None
        try:
            while not self.stop_event.is_set():
                mono, wall = time.monotonic(), time.time()
                if previous_mono is not None:
                    gap = max(mono - previous_mono, wall - previous_wall)
                    if gap > self.config["maximum_sample_gap_seconds"]:
                        self.failures.append({"code": "SAMPLING_INTERRUPTED", "gap_seconds": gap})
                previous_mono, previous_wall = mono, wall
                values = []
                for name, process in self.processes():
                    if process is not None and process.process.poll() is not None:
                        continue
                    pid = os.getpid() if process is None else process.process.pid
                    sample = process_resources(pid)
                    if process is not None and process.process.poll() is not None:
                        continue
                    sample.update(name=name, elapsed_seconds=mono - self.started)
                    prior = self.previous_cpu.get(name)
                    if prior and sample["cpu_seconds"] is not None and mono > prior[0]:
                        sample["cpu_percent"] = max(0, (sample["cpu_seconds"] - prior[1]) / (mono - prior[0]) * 100)
                        if self.phase() == "measuring":
                            self.cpu.setdefault(name, Trend()).add(mono - self.started, sample["cpu_percent"])
                    if sample["cpu_seconds"] is not None:
                        self.previous_cpu[name] = (mono, sample["cpu_seconds"])
                    else:
                        self.unavailable[name + ":cpu_seconds"] += 1
                    for metric in ("rss_bytes", "fds", "threads"):
                        value = sample[metric]
                        if value is None:
                            self.unavailable[name + ":" + metric] += 1
                        if value is not None:
                            key = name + ":" + metric
                            self.peaks[key] = max(self.peaks.get(key, 0), value)
                            limit_name = {"rss_bytes": "rss_bytes_per_process", "fds": "fd_per_process",
                                          "threads": "threads_per_process"}[metric]
                            limit = self.config["thresholds"][limit_name]
                            require(limit is None or value <= limit, "RESOURCE_PEAK", key + " exceeded its limit")
                            if self.phase() == "measuring":
                                self.trends.setdefault(key, Trend()).add(mono - self.started, value)
                    values.append(sample)
                self.log.emit({"sequence": self.samples, "monotonic_seconds": mono,
                               "unix_seconds": wall, "phase": self.phase(), "processes": values})
                self.samples += 1
                self.stop_event.wait(max(0, self.config["sample_seconds"] - (time.monotonic() - mono)))
        except BaseException as error:
            self.failures.append({"code": getattr(error, "code", "RESOURCE_SAMPLER"),
                                  "type": type(error).__name__})

    def finish(self):
        self.stop_event.set()
        self.thread.join(10)
        require(not self.thread.is_alive(), "RESOURCE_SAMPLER", "resource sampler did not stop")
        self.log.close()
        return {"samples": self.samples, "failures": self.failures, "peaks": self.peaks,
                "unavailable_counts": dict(self.unavailable),
                "trends": {name: value.report() for name, value in self.trends.items()},
                "cpu_percent": {name: value.report() for name, value in self.cpu.items()},
                "method": "OS PID measurements; online least-squares slope over measurement phase"}


class Histogram:
    """Fixed 320 log buckets; reported quantiles are 1.1-wide upper bounds."""
    def __init__(self):
        self.counts = [0] * 320
        self.total = 0
        self.maximum = 0.0

    def add(self, seconds):
        require(math.isfinite(seconds) and seconds >= 0, "CLOCK_INVALID", "invalid latency")
        index = min(319, max(0, math.ceil(math.log(max(1, seconds * 1e6), 1.1))))
        self.counts[index] += 1
        self.total += 1
        self.maximum = max(self.maximum, seconds)

    def report(self):
        output = {"count": self.total, "maximum_seconds": self.maximum,
                  "method": "1us origin, base-1.1 log buckets; upper-bound quantiles"}
        for quantile in (50, 95, 99):
            rank = math.ceil(self.total * quantile / 100)
            cumulative = 0
            result = None
            if rank:
                for index, count in enumerate(self.counts):
                    cumulative += count
                    if cumulative >= rank:
                        result = 1.1**index / 1e6
                        break
            output[f"p{quantile}_upper_seconds"] = result
        return output


class Metrics:
    def __init__(self, role, phase=lambda: None, inflight_phase=None):
        self.role = role
        self.phase = phase
        self.inflight_phase = inflight_phase or phase
        self.lock = threading.Lock()
        self.counters = Counter()
        self.epochs = {}
        self.latencies = {}
        self.measured_counters = Counter()
        self.measured_latencies = {}
        self.last_success = {}
        self.errors = []
        self.queue_getters = {}
        self.queue_peaks = {}
        self.inflight = {}

    def observe_inflight(self, measuring):
        # Also sample requests that began during warmup and remain in flight
        # when measurement begins. Entry/exit observations catch sub-sample peaks.
        if measuring:
            for value in self.inflight.values():
                value["measurement_peak"] = max(value["measurement_peak"], value["current"])

    @contextlib.contextmanager
    def track_inflight(self, name):
        measuring = self.inflight_phase() == "measuring"
        with self.lock:
            value = self.inflight.setdefault(name, {"current": 0, "peak": 0, "measurement_peak": 0,
                "started": 0, "finished": 0, "measurement_started": 0, "measurement_finished": 0})
            value["current"] += 1
            value["started"] += 1
            value["measurement_started"] += int(measuring)
            value["peak"] = max(value["peak"], value["current"])
            self.observe_inflight(measuring)
        try:
            yield
        finally:
            measuring = self.inflight_phase() == "measuring"
            with self.lock:
                # Observe before decrementing, including warmup-to-measurement
                # requests and calls that finish between periodic snapshots.
                self.observe_inflight(measuring)
                value["current"] -= 1
                value["finished"] += 1
                value["measurement_finished"] += int(measuring)

    def increment(self, name, count=1):
        measuring = self.phase() == "measuring"
        with self.lock:
            self.counters[name] += count
            if measuring:
                self.measured_counters[name] += count

    def success(self, name, epoch, seconds=None):
        measuring = self.phase() == "measuring"
        with self.lock:
            self.counters[name] += 1
            self.epochs.setdefault(str(epoch), Counter())[name] += 1
            self.last_success[name] = time.time_ns()
            if seconds is not None:
                self.latencies.setdefault(name, Histogram()).add(seconds)
            if measuring:
                self.measured_counters[name] += 1
                if seconds is not None:
                    self.measured_latencies.setdefault(name, Histogram()).add(seconds)

    def error(self, operation, error, expected=False):
        measuring = self.phase() == "measuring"
        with self.lock:
            names = ["expected_fault_errors" if expected else "unexpected_errors", operation + "_errors"]
            if "Timeout" in type(error).__name__:
                names.append(operation + "_timeouts")
            for name in names:
                self.counters[name] += 1
                if measuring:
                    self.measured_counters[name] += 1
            if sum(item["expected_fault"] == expected for item in self.errors) < 16:
                self.errors.append({"operation": operation, "type": type(error).__name__,
                                    "code": getattr(error, "code", None), "expected_fault": expected,
                                    "unix_ns": time.time_ns(), "message": str(error)[:500]})

    def snapshot(self):
        measuring = self.inflight_phase() == "measuring"
        with self.lock:
            self.observe_inflight(measuring)
            output = {"role": self.role, "pid": os.getpid(), "unix_ns": time.time_ns(),
                      "counters": dict(self.counters),
                      "epochs": {epoch: dict(counts) for epoch, counts in self.epochs.items()},
                      "latencies": {name: values.report() for name, values in self.latencies.items()},
                      "measurement_counters": dict(self.measured_counters),
                      "measurement_latencies": {name: values.report() for name, values in self.measured_latencies.items()},
                      "last_success_unix_ns": dict(self.last_success), "errors": list(self.errors),
                      "inflight": {name: dict(value) for name, value in self.inflight.items()}}
        queues = {}
        for name, getter in list(self.queue_getters.items()):
            try:
                queues[name] = getter()
            except Exception as error:
                queues[name] = {"unavailable": type(error).__name__}
        output["queues"] = queues
        for name, stats in queues.items():
            if not isinstance(stats, dict):
                continue
            peaks = self.queue_peaks.setdefault(name, {})
            for key in ("queued", "received_total", "dropped_total"):
                if isinstance(stats.get(key), (int, float)):
                    peaks[key] = max(peaks.get(key, 0), stats[key])
            for key in ("queued_bytes", "reserved_bytes"):
                value = (stats.get("bytes") or {}).get(key)
                if isinstance(value, (int, float)):
                    peaks[key] = max(peaks.get(key, 0), value)
        output["sampled_queue_peaks"] = {key: dict(value) for key, value in self.queue_peaks.items()}
        return output


class Control:
    def __init__(self, path):
        self.path = Path(path)
        self.lock = threading.Lock()
        self.cached = None
        self.read_at = 0

    def read(self, force=False):
        with self.lock:
            if force or self.cached is None or time.monotonic() - self.read_at >= .02:
                self.cached = load_json(self.path)
                self.read_at = time.monotonic()
            return self.cached

    def expected_fault(self, started_ns, ended_ns):
        return any(item["started_unix_ns"] <= ended_ns and
                   item["deadline_unix_ns"] >= started_ns for item in self.read(True)["faults"])


class SequenceLedger:
    """Exact duplicate detection with a predeclared bounded bitmap."""
    def __init__(self, maximum):
        require(0 < maximum <= 50_000_000, "WORKLOAD_CAPACITY", "request ledger exceeds its declared bound")
        self.maximum = maximum
        self.bits = bytearray((maximum + 8) // 8)
        self.lock = threading.Lock()

    def mark(self, sequence):
        require(type(sequence) is int and 1 <= sequence <= self.maximum,
                "REQUEST_ID_RANGE", "request sequence is outside its bounded ledger")
        byte, bit = divmod(sequence, 8)
        with self.lock:
            duplicate = bool(self.bits[byte] & 1 << bit)
            self.bits[byte] |= 1 << bit
        return duplicate


def object_schema(**fields):
    return {"type": "object", "properties": fields, "required": list(fields), "additionalProperties": False}


STRING = {"type": "string"}
INTEGER = {"type": "integer", "minimum": 0}
PAYLOAD = object_schema(run_id=STRING, instance=STRING, epoch=INTEGER, sequence=INTEGER,
                        value=INTEGER, sent_unix_ns=INTEGER, padding=STRING)
GOAL = object_schema(run_id=STRING, epoch=INTEGER, sequence=INTEGER, steps=INTEGER)
PROGRESS = object_schema(run_id=STRING, instance=STRING, epoch=INTEGER, sequence=INTEGER,
                         completed=INTEGER, total=INTEGER)
DEFINITIONS = {
    "topic": {"name": "aos_soak/msg/Payload", "kind": "msg", "message": PAYLOAD},
    "service": {"name": "aos_soak/srv/Echo", "kind": "srv", "request": PAYLOAD, "response": PAYLOAD},
    "action": {"name": "aos_soak/action/Accumulate", "kind": "action", "goal": GOAL,
               "result": {**PROGRESS, "default": {"run_id": "", "instance": "", "epoch": 0,
                           "sequence": 0, "completed": 0, "total": 0}}, "feedback": PROGRESS},
}


def payload(run_id, instance, epoch, sequence, size):
    value = {"run_id": run_id, "instance": instance, "epoch": epoch, "sequence": sequence,
             "value": sequence * 3 + 1, "sent_unix_ns": time.time_ns(), "padding": ""}
    overhead = len(json.dumps(value, separators=(",", ":")).encode())
    require(overhead <= size, "PAYLOAD_SIZE", "declared payload is too small for the envelope")
    value["padding"] = "x" * (size - overhead)
    return value


def validate_payload(value, run_id, instance, size):
    require(value["run_id"] == run_id and value["instance"] == instance and
            value["value"] == value["sequence"] * 3 + 1 and
            value["padding"] == "x" * len(value["padding"]) and
            len(json.dumps(value, separators=(",", ":")).encode()) == size,
            "PAYLOAD_MISMATCH", "business payload, identity or serialized byte count differs")


def clean_environment():
    environment = os.environ.copy()
    for key in list(environment):
        if key.startswith("AOS_") or key in ("PYTHONPATH", "PYTHONHOME"):
            environment.pop(key)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment["RUST_LOG"] = "warn"
    return environment


def security_policy(config, namespace):
    """Finite workload identities, compiled by the production policy compiler."""
    def node(name, ns=namespace):
        return {"name": name, "namespace": ns}

    provider, client, managed = (node(name) for name in ("provider", "client", "managed"))
    subjects = {name: {"id": name, "common_name": name, "nodes": [], "endpoints": [],
                       "systems": [], "graph_nodes": []} for name in ("provider", "client")}

    def owned(subject, identity, operations=("parameters_provide",)):
        subjects[subject]["nodes"].append(identity)
        if operations:
            subjects[subject]["systems"].append({"node": identity, "target": identity, "operations": list(operations)})

    def endpoint(subject, identity, kind, name, operations, qos=None):
        subjects[subject]["endpoints"].append({"node": identity, "namespace": namespace,
            "name": namespace + "/" + name, "interface": DEFINITIONS[kind], "operations": operations, "qos": qos})

    owned("provider", provider)
    owned("client", client)
    subjects["client"]["graph_nodes"].append(provider)
    for component, kind, name, provide, consume in (
        ("small_topic", "topic", "small", ["publish"], ["subscribe"]),
        ("large_topic", "topic", "large", ["publish"], ["subscribe"]),
        ("service", "service", "echo", ["provide"], ["call"]),
        ("action", "action", "work", ["provide"], ["send_goal", "get_result", "cancel_goal", "feedback", "status"]),
    ):
        if active(config, component):
            endpoint("provider", provider, kind, name, provide)
            endpoint("client", client, kind, name, consume)
    if active(config, "management"):
        owned("provider", managed, ("parameters_provide", "lifecycle_provide"))
        subjects["client"]["graph_nodes"].append(managed)
        subjects["client"]["systems"].extend([
            {"node": client, "target": provider, "operations": ["parameters_read", "parameters_write"]},
            {"node": client, "target": managed, "operations": ["lifecycle_read", "lifecycle_change"]},
        ])
    if active(config, "churn"):
        for index in range(config["workload"]["churn"]["rounds"]):
            identity = node("churn_" + str(index))
            owned("client", identity)
            endpoint("client", identity, "topic", identity["name"], ["publish", "subscribe"])
    if active(config, "bag"):
        recorder = node("aos_bag_recorder", "/")
        owned("client", recorder, ())
        subjects["client"]["systems"].append({"node": recorder, "target": provider, "operations": ["describe"]})
        endpoint("client", recorder, "topic", "small", ["subscribe"])
        endpoint("client", recorder, "action", "work", ["feedback", "status"])
    for kind in ("sigint", "context_close"):
        identity = node("boundary_" + kind)
        owned("client", identity)
        endpoint("client", identity, "service", "absent_" + kind, ["call"])
    return {"schema_version": 1, "domain_id": config["domain_id"], "subjects": list(subjects.values())}


def prepare_tls(config, namespace, port, output, pki):
    """Reuse C18's ephemeral OpenSSL fixture and native security serve contract.

    Source: platform_security_probe.py at 2fbbbdc9f36d75d851e844a00e3925699c0e146d.
    Only certificate metadata and paths enter evidence; private key bytes stay
    in a mode-0700 temporary directory and are deleted after owned children exit.
    """
    import aos_sdk as aos
    require(config["duration_seconds"] + config["warmup_seconds"] < 36 * 3600,
            "TLS_FIXTURE_DURATION", "local test credentials expire after two days; use a run shorter than 36 hours")
    pki.chmod(0o700)

    def openssl(*command):
        return subprocess.run(["openssl", *command], cwd=pki, capture_output=True, check=True, timeout=15).stdout.decode()

    openssl("req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes", "-sha256",
            "-days", "2", "-subj", "/CN=AOS C20 local test CA", "-keyout", "ca.key", "-out", "ca.pem",
            "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign")
    for serial, name in enumerate(("server", "provider", "client"), 1):
        openssl("req", "-new", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes", "-subj",
                "/CN=" + name, "-keyout", name + ".key", "-out", name + ".csr")
        (pki / "leaf.ext").write_text("basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyAgreement\n"
            "extendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=IP:127.0.0.1,DNS:localhost\n")
        openssl("x509", "-req", "-sha256", "-days", "2", "-in", name + ".csr", "-CA", "ca.pem", "-CAkey", "ca.key",
                "-set_serial", str(serial), "-extfile", "leaf.ext", "-out", name + ".pem")
    for key in pki.glob("*.key"):
        key.chmod(0o600)
    write_json(output / "public-certificate-metadata.json", {name: openssl("x509", "-in", name + ".pem", "-noout",
               "-subject", "-issuer", "-serial", "-dates", "-sha256", "-fingerprint").splitlines()
               for name in ("ca", "server", "provider", "client")})

    def files(name):
        return {"schema_version": 1, "root_ca_certificate": str(pki / "ca.pem"),
                "certificate": str(pki / (name + ".pem")), "private_key": str(pki / (name + ".key"))}

    policy = security_policy(config, namespace)
    compiled = aos.compile_security_policy(policy)
    write_json(output / "security-policy.json", policy)
    write_json(output / "compiled-security-policy.json", compiled)
    endpoint = "tls/127.0.0.1:" + str(port)
    profiles = {}
    inputs = {}
    for name in ("provider", "client"):
        path = pki / (name + "-profile.json")
        profile = files(name) | {"domain_id": config["domain_id"], "router_endpoints": [endpoint], "connect_timeout_ms": 3000}
        write_json(path, profile)
        write_json(output / ("security-" + name + "-profile.json"), profile)
        profiles[name] = str(path)
        inputs[name] = {"path": str(path), "sha256": digest(path)}
    request = pki / "router-request.json"
    write_json(request, {"router": files("server") | {"listen_endpoints": [endpoint]}, "policy": policy,
                         "startup_timeout": 10, "shutdown_timeout": 5})
    write_json(output / "security-router-request.json", load_json(request))
    inputs["router"] = {"path": str(request), "sha256": digest(request)}
    return {"profiles": profiles, "request": str(request), "policy_sha256": digest(output / "security-policy.json"),
            "input_files": inputs,
            "compiled_policy_sha256": digest(output / "compiled-security-policy.json"), "endpoint": endpoint,
            "fixture_source_sha": "2fbbbdc9f36d75d851e844a00e3925699c0e146d"}


def open_context(aos, manifest, role):
    if transport(manifest["config"]) == "mtls":
        return aos.Context(security_profile=Path(manifest["security"]["profiles"][role]))
    return aos.Context(config=Path(manifest["session_config"]), domain_id=manifest["config"]["domain_id"])


class Worker:
    def __init__(self, args):
        import aos_sdk as aos
        self.aos = aos
        self.args = args
        self.output = args.output.resolve()
        self.manifest = load_json(self.output / "run.json")
        self.config = self.manifest["config"]
        self.role = args.role
        self.binding = verify_artifacts(self.manifest["binding"]["paths"]["proof"],
                                        self.manifest["source_sha"], self.manifest["binding"]["paths"]["cli"],
                                        self.manifest["binding"]["paths"]["router"])
        require(digest(SCRIPT) == self.manifest["harness_sha256"], "HARNESS_CHANGED", "worker script changed")
        require(self.binding == self.manifest["binding"], "ARTIFACT_MISMATCH", "worker artifact binding differs")
        self.control = Control(self.output / "control.json")
        self.metrics = Metrics(self.role, lambda: self.control.read()["phase"],
                               lambda: self.control.read(force=True)["phase"])
        self.log = BoundedJsonl(self.output / (self.role + "-samples.jsonl"),
                                self.config["storage"]["worker_log_bytes"])
        self.stop = threading.Event()
        self.threads = []
        self.next_sequence = Counter()
        self.sequence_lock = threading.Lock()
        self.runtime_ready = threading.Event()
        self.context = open_context(aos, self.manifest, self.role)
        self.node = aos.Node(self.role, self.manifest["namespace"], context=self.context)
        thread_count = max(8, self.config["workload"]["action"]["concurrency"] + 4)
        self.executor = aos.MultiThreadedExecutor(thread_count, context=self.context)
        self.executor.add_node(self.node)
        self.spinner = threading.Thread(target=self.executor.spin, name=self.role + "-executor", daemon=True)
        self.spinner.start()
        self.interfaces = {name: aos.Interface.load(definition) for name, definition in DEFINITIONS.items()}
        self.instance = uuid.uuid4().hex
        self.bag = None
        self.managed = None
        self.final = None
        self.started_at = time.monotonic()

    def sequence(self, kind):
        with self.sequence_lock:
            self.next_sequence[kind] += 1
            return self.next_sequence[kind]

    def thread(self, name, function):
        def execute():
            try:
                function()
            except BaseException as error:
                self.metrics.error(name, error)
                self.stop.set()
        worker = threading.Thread(target=execute, name=self.role + "-" + name, daemon=True)
        self.threads.append(worker)
        worker.start()
        return worker

    def expected(self, operation, started_ns, error):
        allowed = (self.aos.WaitTimeoutError, self.aos.NoResponseError,
                   self.aos.CommunicationError, TimeoutError)
        expected = isinstance(error, allowed) and self.control.expected_fault(started_ns, time.time_ns())
        self.metrics.error(operation, error, expected)
        if not expected:
            raise error

    def ready(self, extra=None):
        record = {"role": self.role, "pid": os.getpid(), "instance": self.instance,
                  "node_id": self.node.get_node_card()["id"], "source_sha": self.manifest["source_sha"],
                  "native_sha256": self.binding["native_sha256"], "unix_ns": time.time_ns(),
                  "node_card": self.node.get_node_card(), **(extra or {})}
        write_json(self.output / (self.role + "-ready.json"), record)
        self.runtime_ready.set()
        self.thread("snapshot", self.snapshots)

    def snapshots(self):
        while not self.stop.is_set():
            state = self.metrics.snapshot()
            state["phase"] = self.control.read()["phase"]
            state["context_ok"] = self.context.ok()
            if self.bag is not None:
                progress = self.bag.progress if hasattr(self.bag, "progress") else self.bag.status
                state["bag"] = {key: progress.get(key) for key in
                                ("state", "counters", "queue_messages", "queue_bytes", "resources_closed")}
                if progress["state"] not in ("recording", "starting", "stopping"):
                    raise ProbeError("BAG_STOPPED_EARLY", "recording stopped before the workload ended")
            self.log.emit(state)
            write_json(self.output / (self.role + "-status.json"), state)
            self.stop.wait(self.config["sample_seconds"])

    def provider(self):
        work = self.config["workload"]
        duration = sum(self.config[name] for name in ("duration_seconds", "warmup_seconds",
                                                     "startup_timeout_seconds", "shutdown_timeout_seconds"))
        services = SequenceLedger(math.ceil(duration * work["service"]["hz"] * 2 + 1000))
        action_rate = work["action"]["concurrency"] * max(1 / work["action"]["duration_seconds"],
                                                         work["action"]["feedback_hz"])
        actions = SequenceLedger(math.ceil(duration * action_rate * 2 + 1000))

        def echo_value(request):
            validate_payload(request, self.manifest["run_id"], self.instance, work["service"]["payload_bytes"])
            duplicate = services.mark(request["sequence"])
            self.metrics.increment("service_duplicate_execution", int(duplicate))
            require(not duplicate, "BUSINESS_REPLAY", "the same Service request executed twice")
            self.metrics.success("service_executed", request["epoch"])
            return request

        def echo(request):
            with self.metrics.track_inflight("service_callbacks"):
                return echo_value(request)

        if active(self.config, "service"):
            service = self.node.create_service(self.interfaces["service"], "echo", echo)
            self.metrics.queue_getters["service"] = lambda: service.queue_stats

        def execute_goal(goal):
            request = goal.request
            require(request["run_id"] == self.manifest["run_id"], "STALE_GOAL", "goal belongs to another run")
            duplicate = actions.mark(request["sequence"])
            self.metrics.increment("action_duplicate_execution", int(duplicate))
            require(not duplicate, "BUSINESS_REPLAY", "the same Action request executed twice")
            self.metrics.success("action_executed", request["epoch"])
            completed = total = 0
            for step in range(1, request["steps"] + 1):
                if goal.is_cancel_requested:
                    goal.canceled()
                    break
                if self.stop.is_set():
                    goal.abort()
                    break
                completed, total = step, total + step
                goal.publish_feedback({"run_id": request["run_id"], "instance": self.instance,
                                       "epoch": request["epoch"], "sequence": request["sequence"],
                                       "completed": completed, "total": total})
                self.metrics.increment("feedback_offered")
                self.stop.wait(1 / work["action"]["feedback_hz"])
            else:
                goal.succeed()
            self.metrics.increment("action_terminal_" + str(int(goal.status)))
            return {"run_id": request["run_id"], "instance": self.instance, "epoch": request["epoch"],
                    "sequence": request["sequence"], "completed": completed, "total": total}

        def execute(goal):
            with self.metrics.track_inflight("action_executions"):
                return execute_goal(goal)

        if active(self.config, "action"):
            self.aos.ActionServer(self.node, self.interfaces["action"], "work", execute,
                                  cancel_callback=lambda _: self.aos.CancelResponse.ACCEPT,
                                  result_timeout=self.config["operation_timeout_seconds"] * 2)
        if active(self.config, "management"):
            self.node.declare_parameter("left", 0)
            self.node.declare_parameter("right", 0)
            self.node.declare_parameter("fixed", 1, self.aos.ParameterDescriptor(read_only=True))
            self.managed = self.aos.LifecycleNode("managed", self.manifest["namespace"], context=self.context)
            self.executor.add_node(self.managed)
            report = self.managed.trigger_configure_async().result(self.config["operation_timeout_seconds"])
            require(report["successful"], "LIFECYCLE_CONFIGURE", "managed provider did not configure")
        publishers = {}
        for kind in ("small", "large"):
            if not active(self.config, kind + "_topic"):
                continue
            profile = work[kind + "_topic"]
            publishers[kind] = self.node.create_publisher(self.interfaces["topic"], kind,
                                                          qos_profile={"depth": profile["depth"]})
        self.ready({"managed_node_id": self.managed.get_node_card()["id"] if self.managed is not None else None,
                    "ledger_bytes": {"service": len(services.bits), "action": len(actions.bits)}})

        def publish(kind):
            profile = work[kind + "_topic"]
            next_at = time.monotonic()
            while not self.stop.is_set():
                epoch = self.control.read()["epoch"]
                sequence = self.sequence(kind)
                message = payload(self.manifest["run_id"], self.instance, epoch, sequence, profile["payload_bytes"])
                started_ns = time.time_ns()
                try:
                    publishers[kind].publish(message)
                    self.metrics.success(kind + "_offered", epoch)
                    self.metrics.increment(kind + "_offered_bytes", profile["payload_bytes"])
                except Exception as error:
                    self.expected("publish_" + kind, started_ns, error)
                next_at += 1 / profile["hz"]
                # Record missed pacing slots; do not create an unbounded catch-up burst.
                now = time.monotonic()
                if next_at < now:
                    skipped = int((now - next_at) * profile["hz"])
                    self.metrics.increment(kind + "_pacing_skipped", skipped)
                    next_at = now
                self.stop.wait(max(0, next_at - time.monotonic()))
        for kind in publishers:
            self.thread("publish-" + kind, lambda kind=kind: publish(kind))

    def client(self):
        work = self.config["workload"]
        timeout = self.config["operation_timeout_seconds"]
        provider = load_json(self.output / "provider-ready.json")
        self.provider_instance = provider["instance"]
        if active(self.config, "service"):
            self.service = self.node.create_client(self.interfaces["service"], "echo")
            require(self.service.wait_for_service(timeout), "READINESS_TIMEOUT", "Service did not become ready")
            self.metrics.queue_getters["service_client"] = lambda: self.service.queue_stats
        if active(self.config, "action"):
            self.action = self.aos.ActionClient(self.node, self.interfaces["action"], "work")
            require(self.action.wait_for_server(timeout), "READINESS_TIMEOUT", "Action did not become ready")
        subscriptions = []
        for index in range(work["small_topic"]["fast_consumers"]):
            subscriptions.append((f"small_fast_{index}", "small", 0))
        for index in range(work["small_topic"]["slow_consumers"]):
            subscriptions.append((f"small_slow_{index}", "small", work["small_topic"]["slow_delay_seconds"]))
        subscriptions.append(("large", "large", 0))
        for name, kind, delay in subscriptions:
            if not active(self.config, kind + "_topic"):
                continue
            subscription = self.node.create_subscription(self.interfaces["topic"], kind,
                                                           qos_profile={"depth": work[kind + "_topic"]["depth"]})
            self.metrics.queue_getters[name] = lambda subscription=subscription: subscription.queue_stats
            self.thread(name, lambda name=name, kind=kind, delay=delay, subscription=subscription:
                        self.receive(name, kind, delay, subscription))
        if active(self.config, "management"):
            self.prepare_management()
        if active(self.config, "bag"):
            self.prepare_bag()
        self.ready()
        if active(self.config, "service"):
            for _ in range(work["service"]["concurrency"]):
                self.thread("service", self.call_services)
        if active(self.config, "action"):
            for _ in range(work["action"]["concurrency"]):
                self.thread("action", self.call_actions)
        if active(self.config, "management"):
            self.thread("management", self.management)
        if active(self.config, "churn"):
            self.thread("churn", self.churn)

    def prepare_bag(self):
        work = self.config["workload"]
        bag = work["bag"]
        self.bag = self.aos.BagRecorder(
            self.context, self.output / "bag", topics=[self.manifest["namespace"] + "/small",
                                                       self.manifest["namespace"] + "/work/_action/feedback",
                                                       self.manifest["namespace"] + "/work/_action/status"],
            queue_messages=bag["queue_messages"], queue_bytes=bag["queue_bytes"],
            adapter_queue_bytes=bag["adapter_queue_bytes"], message_bytes=bag["message_bytes"],
            qos={"depth": 10, "reliability": "best_effort", "durability": "volatile"},
            storage={"total_bytes": bag["total_bytes"], "shard_bytes": min(256 * 1024**2, bag["total_bytes"] // 4),
                     # Retain storage's default shard counts so its index budget
                     # remains valid as reader resource accounting evolves.
                     "chunk_bytes": bag["message_bytes"] + 128 * 1024, "chunk_interval_ms": 500},
        )

    def receive(self, name, kind, delay, subscription):
        last = None
        while not self.stop.is_set():
            try:
                message = subscription.recv(.2)
            except self.aos.WaitTimeoutError:
                continue
            validate_payload(message, self.manifest["run_id"], self.provider_instance,
                             self.config["workload"][kind + "_topic"]["payload_bytes"])
            sequence = message["sequence"]
            if last is not None:
                require(sequence > last, "TOPIC_REPLAY_OR_REORDER", f"{name}: non-increasing Topic sequence")
                self.metrics.increment(name + "_sequence_gaps", sequence - last - 1)
            last = sequence
            # Both processes use this host's wall clock. Cross-machine delay
            # requires a separately proved clock relationship and is not inferred.
            seconds = (time.time_ns() - message["sent_unix_ns"]) / 1e9
            self.metrics.success(name + "_received", message["epoch"], seconds)
            self.metrics.increment(name + "_received_bytes", self.config["workload"][kind + "_topic"]["payload_bytes"])
            self.stop.wait(delay)

    def call_services(self):
        profile = self.config["workload"]["service"]
        period = profile["concurrency"] / profile["hz"]
        while not self.stop.is_set():
            started = time.monotonic()
            started_ns = time.time_ns()
            epoch = self.control.read()["epoch"]
            sequence = self.sequence("service")
            request = payload(self.manifest["run_id"], self.provider_instance, epoch, sequence, profile["payload_bytes"])
            self.metrics.increment("service_sent")
            try:
                with self.metrics.track_inflight("service_calls"):
                    response = self.service.call(request, timeout=self.config["operation_timeout_seconds"])
                require(response == request, "SERVICE_RESULT", "Service result does not match its unique request")
                self.metrics.success("service_ok", epoch, time.monotonic() - started)
            except Exception as error:
                self.expected("service", started_ns, error)
            self.metrics.increment("service_pacing_skipped", max(0, int((time.monotonic() - started) / period) - 1))
            self.stop.wait(max(0, period - (time.monotonic() - started)))

    def call_actions(self):
        profile = self.config["workload"]["action"]
        timeout = self.config["operation_timeout_seconds"]
        while not self.stop.is_set():
            sequence = self.sequence("action")
            epoch = self.control.read()["epoch"]
            canceled = sequence % profile["cancel_every"] == 0
            duration = profile["cancel_duration_seconds"] if canceled else profile["duration_seconds"]
            request = {"run_id": self.manifest["run_id"], "epoch": epoch, "sequence": sequence,
                       "steps": max(2, round(duration * profile["feedback_hz"]))}
            started, started_ns = time.monotonic(), time.time_ns()
            first_feedback = threading.Event()
            last_feedback = [0]

            def feedback(message, request=request, epoch=epoch, last_feedback=last_feedback,
                         first_feedback=first_feedback):
                value = message["feedback"]
                try:
                    self.validate_action_value(value, request)
                    require(value["completed"] > last_feedback[0], "FEEDBACK_REPLAY", "feedback did not advance")
                    self.metrics.increment("action_feedback_sequence_gaps", value["completed"] - last_feedback[0] - 1)
                    last_feedback[0] = value["completed"]
                    self.metrics.success("action_feedback", epoch)
                    first_feedback.set()
                except BaseException as error:
                    self.metrics.error("feedback", error)
                    self.stop.set()

            self.metrics.increment("action_sent")
            with self.metrics.track_inflight("action_requests"):
                try:
                    goal = self.action.send_goal_async(request, feedback_callback=feedback, timeout=timeout).result(timeout + 1)
                    require(goal.accepted, "ACTION_REJECTED", "a valid load goal was rejected")
                    self.metrics.success("action_accepted", epoch, time.monotonic() - started)
                    future = goal.get_result_async(timeout=duration + timeout)
                    if canceled:
                        if not first_feedback.wait(timeout):
                            raise self.aos.WaitTimeoutError("cancel case received no feedback")
                        cancel_started = time.monotonic()
                        response = goal.cancel_goal_async(timeout=timeout).result(timeout + 1)
                        require(goal.goal_id in [item["goal_id"] for item in response["goals_canceling"]],
                                "CANCEL_NOT_ACCEPTED", "cancel response omitted the actual goal")
                        self.metrics.success("action_cancel_accepted", epoch, time.monotonic() - cancel_started)
                    result_started = time.monotonic()
                    result = future.result(duration + timeout + 1)
                    self.metrics.success("action_result_wait", epoch, time.monotonic() - result_started)
                    if result["status"] == int(self.aos.GoalStatus.UNKNOWN):
                        raise self.aos.NoResponseError("Action result is UNKNOWN after the transport fault")
                    require(result["status"] == int(self.aos.GoalStatus.CANCELED if canceled else self.aos.GoalStatus.SUCCEEDED),
                            "ACTION_TERMINAL", "Action terminal status differs from the requested scenario")
                    self.validate_action_value(result["result"], request)
                    completed = result["result"]["completed"]
                    require(0 < completed < request["steps"] if canceled else completed == request["steps"],
                            "ACTION_RESULT", "Action result completed the wrong number of steps")
                    if canceled:
                        self.metrics.success("action_cancel_terminal", epoch, time.monotonic() - cancel_started)
                    # A second read of the same terminal result measures GetResult
                    # control latency independently of the 30s goal duration.
                    terminal_started = time.monotonic()
                    terminal = goal.get_result_async(timeout=timeout).result(timeout + 1)
                    require(terminal == result, "ACTION_CACHED_RESULT", "terminal GetResult changed the exact result")
                    self.metrics.success("action_get_result_terminal", epoch, time.monotonic() - terminal_started)
                    self.metrics.success("action_canceled" if canceled else "action_succeeded", epoch,
                                         time.monotonic() - started)
                    self.metrics.increment("action_feedback_terminal_tail_missing", max(0, completed - last_feedback[0]))
                except Exception as error:
                    self.expected("action", started_ns, error)
                    # An unknown reply does not stop provider execution. Keep
                    # this load slot through the known workload's duration and
                    # request timeout before issuing another unique goal. This
                    # conservative pacing window is not proof of a terminal
                    # state; it prevents reconnect errors from flooding the
                    # provider with extra long executions.
                    self.stop.wait(max(0, duration + timeout - (time.monotonic() - started)))
            # A disconnected client can fail synchronously. Bound fresh goal
            # issuance as well as concurrency; every attempt has a new ID.
            self.stop.wait(max(0, 1 / profile["feedback_hz"] - (time.monotonic() - started)))

    def validate_action_value(self, value, request):
        completed = value["completed"]
        require(value["run_id"] == request["run_id"] and value["instance"] == self.provider_instance and
                value["sequence"] == request["sequence"] and value["epoch"] == request["epoch"] and
                0 <= completed <= request["steps"] and value["total"] == completed * (completed + 1) // 2,
                "ACTION_PAYLOAD", "Action result/feedback has invalid identity or arithmetic")

    def prepare_management(self):
        self.management_clients = {}
        for name in ("set_parameters_atomically", "get_parameters"):
            client = self.node.create_client(self.aos.parameter_service_interface(name),
                                             self.manifest["namespace"] + "/provider/" + name,
                                             qos_profile="parameters")
            self.management_clients[name] = client
        for name in ("change_state", "get_state"):
            client = self.node.create_client(self.aos.lifecycle_service_interface(name),
                                             self.manifest["namespace"] + "/managed/" + name,
                                             qos_profile="lifecycle_services")
            self.management_clients[name] = client
        for name, client in self.management_clients.items():
            require(client.wait_for_service(self.config["operation_timeout_seconds"]),
                    "MANAGEMENT_READINESS", name + " is unavailable")
            self.metrics.queue_getters[name] = lambda client=client: client.queue_stats

    def management(self):
        clients = self.management_clients
        timeout = self.config["operation_timeout_seconds"]
        while not self.stop.is_set():
            started_ns = time.time_ns()
            epoch = self.control.read()["epoch"]
            sequence = self.sequence("management")
            try:
                parameters = [self.aos.Parameter("left", value=sequence).to_dict(),
                              self.aos.Parameter("right", value=sequence * 2).to_dict()]
                changed = clients["set_parameters_atomically"].call({"parameters": parameters}, timeout)["result"]
                require(changed["successful"], "PARAMETER_UPDATE", "valid atomic parameter update failed")
                before = clients["get_parameters"].call({"names": ["left", "right"]}, timeout)
                require(before["error"] is None and [item["value"] for item in before["value"]] == [sequence, sequence * 2],
                        "PARAMETER_ATOMICITY", "atomic values disagree")
                rejected = clients["set_parameters_atomically"].call({"parameters": [
                    self.aos.Parameter("left", value=sequence + 1).to_dict(),
                    self.aos.Parameter("fixed", value=2).to_dict(),
                ]}, timeout)["result"]
                require(not rejected["successful"], "PARAMETER_READ_ONLY", "read-only parameter was modified")
                after = clients["get_parameters"].call({"names": ["left", "right"]}, timeout)
                require(after == before, "PARAMETER_ATOMICITY", "rejected update partially changed the state")
                self.metrics.success("parameter_ok", epoch)
                state = clients["get_state"].call({}, timeout)
                # Resume from the actual state after a timed-out control request;
                # never blindly replay a possibly applied transition.
                label = state["current_state"]["label"]
                for transition, identifier, target in (("activate", 3, "active"), ("deactivate", 4, "inactive")):
                    if label == target:
                        continue
                    response = clients["change_state"].call({"transition": {"id": identifier, "label": transition}}, timeout)
                    require(response["success"], "LIFECYCLE_CHANGE", "lifecycle transition failed")
                    label = clients["get_state"].call({}, timeout)["current_state"]["label"]
                    require(label == target, "LIFECYCLE_STATE", "lifecycle state did not match the transition")
                self.metrics.success("lifecycle_ok", epoch)
            except Exception as error:
                self.expected("management", started_ns, error)
            self.stop.wait(self.config["workload"]["management"]["interval_seconds"])

    def churn(self):
        profile = self.config["workload"]["churn"]
        # Warmup must not consume the baseline's 100 measured create/destroy cycles.
        while self.control.read()["phase"] != "measuring":
            if self.stop.wait(.05):
                return
        for round_ in range(profile["rounds"]):
            if self.stop.is_set():
                return
            started_ns = time.time_ns()
            temporary = None
            try:
                temporary = self.aos.Node("churn_" + str(round_), self.manifest["namespace"], context=self.context)
                sub = temporary.create_subscription(self.interfaces["topic"], "churn_" + str(round_))
                pub = temporary.create_publisher(self.interfaces["topic"], "churn_" + str(round_))
                pub.wait_for_subscribers(self.config["operation_timeout_seconds"])
                message = payload(self.manifest["run_id"], self.provider_instance,
                                  self.control.read()["epoch"], round_ + 1, 1024)
                pub.publish(message)
                require(sub.recv(self.config["operation_timeout_seconds"]) == message,
                        "CHURN_PAYLOAD", "churn Topic did not complete")
                try:
                    sub.recv(.001)
                except self.aos.WaitTimeoutError:
                    self.metrics.increment("churn_timeout")
                else:
                    raise ProbeError("CHURN_TIMEOUT", "empty churn subscription did not time out")
                temporary.destroy_node()
                temporary = None
                require(sub.queue_stats["closed"] and sub.queue_stats["queued"] == 0,
                        "CHURN_QUEUE", "destroyed subscription retained queued samples")
                self.metrics.increment("churn_closed")
            except Exception as error:
                self.expected("churn", started_ns, error)
            finally:
                if temporary is not None:
                    temporary.destroy_node()
            if self.stop.wait(profile["interval_seconds"]):
                return

    def close(self):
        self.stop.set()
        deadline = time.monotonic() + self.config["shutdown_timeout_seconds"]
        cleanup_errors = []
        for worker in self.threads:
            worker.join(max(0, deadline - time.monotonic()))
        pending = [worker.name for worker in self.threads if worker.is_alive()]
        if pending:
            cleanup_errors.append({"code": "WORKERS_REMAIN", "threads": pending})
        if self.bag is not None:
            try:
                report = self.bag.close(max(.1, deadline - time.monotonic()))
                require(report["status"]["resources_closed"] and report["manifest"]["clean_close"],
                        "BAG_NOT_CLOSED", "bag did not close both resources and file")
                write_json(self.output / "bag-final.json", report)
            except BaseException as error:
                cleanup_errors.append({"code": getattr(error, "code", "BAG_CLOSE"), "type": type(error).__name__})
        before = process_resources(os.getpid())
        try:
            self.context.shutdown()
            clean = self.executor.shutdown(timeout_sec=max(.1, deadline - time.monotonic()))
            self.spinner.join(max(.1, deadline - time.monotonic()))
            if self.managed is not None:
                self.managed.destroy_node()
            self.node.destroy_node()
            require(clean and not self.spinner.is_alive() and not self.context.ok(),
                    "CONTEXT_NOT_CLOSED", "Context/Executor did not stop")
        except BaseException as error:
            cleanup_errors.append({"code": getattr(error, "code", "CONTEXT_CLOSE"), "type": type(error).__name__})
        time.sleep(self.config["quiet_seconds"])
        final = self.metrics.snapshot()
        final.update(cleanup_errors=cleanup_errors, context_closed=not self.context.ok(),
                     resource_before_close=before, resource_after_close=process_resources(os.getpid()),
                     pending_threads=[worker.name for worker in self.threads if worker.is_alive()],
                     finished_unix_ns=time.time_ns())
        self.log.emit(final)
        self.log.close()
        write_json(self.output / (self.role + "-final.json"), final)
        self.final = final
        return not cleanup_errors and not final["counters"].get("unexpected_errors", 0)

    def run(self):
        try:
            getattr(self, self.role)()
            while not self.stop.wait(.05):
                if self.control.read()["stop_" + self.role]:
                    break
        except BaseException as error:
            self.metrics.error("worker_main", error)
        return 0 if self.close() else 1


def business_keys(config):
    keys = []
    if active(config, "small_topic"):
        keys.extend(f"small_fast_{index}_received" for index in range(config["workload"]["small_topic"]["fast_consumers"]))
    if active(config, "large_topic"):
        keys.append("large_received")
    if active(config, "service"):
        keys.append("service_ok")
    if active(config, "action"):
        keys.extend(("action_succeeded", "action_canceled"))
    return keys


def epoch_recovered(config, status, epoch):
    counts = status.get("epochs", {}).get(str(epoch), {})
    return all(counts.get(key, 0) > 0 for key in business_keys(config))


def evaluate(config, result):
    """A pure, fail-closed gate; offline mutation tests can falsify each input."""
    failures = list(result.get("failures", []))

    def check(value, code, detail=None):
        if not value:
            failures.append({"code": code, **({"detail": detail} if detail else {})})

    check(result.get("finished_normally") is True, "INCOMPLETE_RUN")
    check(result.get("interrupted") is False, "INTERRUPTED")
    check(result.get("measurement_seconds", 0) >= config["duration_seconds"], "DURATION_TOO_SHORT")
    binding = result.get("binding", {})
    check(bool(binding) and result.get("binding_after") == binding, "ARTIFACT_CHANGED")
    check(result.get("harness_after") == result.get("harness_sha256") and
          bool(result.get("harness_sha256")), "HARNESS_CHANGED")
    check(result.get("config_after") == result.get("input_config_sha256") and
          bool(result.get("input_config_sha256")), "CONFIG_CHANGED")
    raw = result.get("raw_evidence") or {}
    check(raw.get("path") == RAW_MANIFEST and bool(HASH.fullmatch(str(raw.get("sha256", "")))),
          "RAW_EVIDENCE_MISSING")
    check(result.get("configured_client_threads") == configured_client_threads(config),
          "CONFIGURED_CONCURRENCY_CHANGED")
    try:
        round_scope(result)
    except ProbeError as error:
        check(False, error.code)
    if config["mode"] == "soak":
        check(result.get("measurement_seconds", 0) >= DAY, "LESS_THAN_24_HOURS")
        check(binding.get("source_sha") == config["freeze"]["source_sha"], "SOURCE_MISMATCH")
        check(binding.get("artifact_sha256") == config["freeze"]["artifact_sha256"], "ARTIFACT_MISMATCH")
        check(result.get("harness_source", {}).get("status") == "", "HARNESS_DIRTY")
        check(bool(result.get("freeze_verification")) and result.get("freeze_verification") == result.get("freeze_after"),
              "FREEZE_EVIDENCE_CHANGED")
    if config["mode"] == "baseline":
        check(result.get("warmup_seconds", 0) >= config["warmup_seconds"] >= 300, "WARMUP_TOO_SHORT")
        check(result.get("measurement_seconds", 0) >= 900, "BASELINE_DURATION")
        check(result.get("harness_source", {}).get("status") == "", "HARNESS_DIRTY")
    resources = result.get("resources", {})
    check(resources.get("samples", 0) >= 3, "RESOURCE_EVIDENCE_MISSING")
    failures.extend(resources.get("failures", []))
    check(not resources.get("unavailable_counts"), "RESOURCE_METRIC_UNAVAILABLE")
    thresholds = config["thresholds"]
    metric_thresholds = {"rss_bytes": "rss_bytes_per_process", "fds": "fd_per_process", "threads": "threads_per_process"}
    growth_thresholds = {"rss_bytes": "rss_growth_bytes_per_hour", "fds": "fd_growth_per_hour", "threads": "threads_growth_per_hour"}
    for name, maximum in resources.get("peaks", {}).items():
        threshold = thresholds[metric_thresholds[name.rsplit(":", 1)[1]]]
        check(threshold is None or maximum <= threshold, "RESOURCE_PEAK", name)
    for name, trend in resources.get("trends", {}).items():
        threshold = thresholds[growth_thresholds[name.rsplit(":", 1)[1]]]
        check(threshold is None or trend["growth_per_hour"] is not None and trend["growth_per_hour"] <= threshold,
              "RESOURCE_GROWTH", name)
    workers = result.get("workers", {})
    for role in ("provider", "client"):
        worker = workers.get(role, {})
        check(bool(worker), "WORKER_EVIDENCE_MISSING", role)
        check(worker.get("context_closed") is True and not worker.get("cleanup_errors") and
              not worker.get("pending_threads"), "WORKER_NOT_CLOSED", role)
        check(worker.get("counters", {}).get("unexpected_errors", 0) == 0, "WORKER_ERROR", role)
        for name, component in INFLIGHT_SCOPES[role].items():
            if not active(config, component):
                continue
            observed = worker.get("inflight", {}).get(name)
            check(isinstance(observed, dict), "INFLIGHT_EVIDENCE_MISSING", role + ":" + name)
            if not isinstance(observed, dict):
                continue
            fields = ("current", "peak", "measurement_peak", "started", "finished", "measurement_started", "measurement_finished")
            valid = all(type(observed.get(key)) is int and observed[key] >= 0 for key in fields)
            check(valid, "INFLIGHT_EVIDENCE_INVALID", role + ":" + name)
            if not valid:
                continue
            check(observed["current"] == 0 and observed["started"] == observed["finished"],
                  "INFLIGHT_NOT_CLOSED", role + ":" + name)
            check(0 < observed["measurement_peak"] <= observed["peak"] <= observed["started"] and
                  observed["measurement_started"] <= observed["started"] and
                  observed["measurement_finished"] <= observed["finished"],
                  "INFLIGHT_EVIDENCE_INVALID", role + ":" + name)
            if role == "client":
                check(observed["peak"] <= configured_client_threads(config)[component],
                      "INFLIGHT_EXCEEDS_CLIENT_THREADS", name)
        for name, stats in worker.get("queues", {}).items():
            check(isinstance(stats, dict) and stats.get("closed") is True and stats.get("queued") == 0,
                  "QUEUE_NOT_CLOSED", role + ":" + name)
    client = workers.get("client", {})
    provider = workers.get("provider", {})
    required_business = business_keys(config)
    if active(config, "management"):
        required_business.extend(("parameter_ok", "lifecycle_ok"))
    if active(config, "churn"):
        required_business.append("churn_closed")
    for key in required_business:
        check(client.get("counters", {}).get(key, 0) > 0, "BUSINESS_EVIDENCE_MISSING", key)
    if config["mode"] == "baseline":
        for key in required_business:
            check(client.get("measurement_counters", {}).get(key, 0) > 0, "MEASURED_BUSINESS_MISSING", key)
    if active(config, "churn"):
        check(client.get("measurement_counters", {}).get("churn_closed", 0) == config["workload"]["churn"]["rounds"],
              "CHURN_INCOMPLETE")
        check(client.get("measurement_counters", {}).get("churn_timeout", 0) == config["workload"]["churn"]["rounds"],
              "CHURN_TIMEOUT_INCOMPLETE")
    for key in ("service_duplicate_execution", "action_duplicate_execution"):
        check(provider.get("counters", {}).get(key, 0) == 0, "BUSINESS_REPLAY", key)
    faults = result.get("faults", [])
    check(len(faults) == len(config["faults"]), "FAULT_EVIDENCE_MISSING")
    for fault in faults:
        check(fault.get("old_process_reaped") and fault.get("port_closed") and
              fault.get("new_pid") is not None and fault.get("new_pid") != fault.get("old_pid") and
              fault.get("recovered") and fault.get("recovery_seconds", math.inf) <= fault["recovery_timeout_seconds"] and
              epoch_recovered(config, client, fault.get("epoch")), "ROUTER_RECOVERY_FAILED")
    latencies = client.get("measurement_latencies", {})
    if config["mode"] == "baseline":
        required_latency = [key for key in business_keys(config) if key.endswith("_received")]
        if active(config, "action"):
            required_latency.extend(("action_get_result_terminal", "action_cancel_terminal", "action_succeeded", "action_canceled"))
        for key in required_latency:
            histogram = latencies.get(key, {})
            check(histogram.get("count", 0) > 0 and all(histogram.get(f"p{q}_upper_seconds") is not None for q in (50, 95, 99)),
                  "LATENCY_EVIDENCE_MISSING", key)
    for key, threshold in (("service_ok", "service_latency_p99_seconds"),
                           ("action_accepted", "action_accept_p99_seconds"),
                           ("action_cancel_accepted", "action_cancel_p99_seconds")):
        if not active(config, "service" if key == "service_ok" else "action"):
            continue
        histogram = latencies.get(key, {})
        check(histogram.get("count", 0) > 0, "LATENCY_EVIDENCE_MISSING", key)
        limit = thresholds[threshold]
        check(limit is None or histogram.get("p99_upper_seconds", math.inf) <= limit, "LATENCY_THRESHOLD", key)
    loss = {}
    measured = client.get("measurement_counters", {})
    names = []
    if active(config, "small_topic"):
        names.extend(f"small_fast_{index}" for index in range(config["workload"]["small_topic"]["fast_consumers"]))
        names.extend(f"small_slow_{index}" for index in range(config["workload"]["small_topic"]["slow_consumers"]))
    if active(config, "large_topic"):
        names.append("large")
    for name in names:
        received, gaps = measured.get(name + "_received", 0), measured.get(name + "_sequence_gaps", 0)
        loss[name] = gaps / (gaps + received) if received + gaps else None
        limit = None if name.startswith("small_slow_") else thresholds[
            "large_topic_loss_fraction" if name == "large" else "fast_topic_loss_fraction"]
        check(limit is None or loss[name] is not None and loss[name] <= limit, "TOPIC_LOSS", name)
    bag = result.get("bag", {})
    if active(config, "bag"):
        check(bag.get("status", {}).get("resources_closed") is True and
              bag.get("manifest", {}).get("clean_close") is True and
              bag.get("status", {}).get("counters", {}).get("written_total", 0) > 0,
              "BAG_EVIDENCE_MISSING")
    for mode in ("sigint", "context_close"):
        check(result.get("boundaries", {}).get(mode, {}).get("passed") is True,
              "BOUNDARY_FAILED", mode)
    exits = result.get("process_exits", {})
    check(bool(exits) and all(item.get("reaped") and not item.get("forced") and item.get("log_complete")
                             for item in exits.values()), "PROCESS_CLEANUP_FAILED")
    for role in ("provider", "client", "boundary_sigint", "boundary_context_close"):
        check(exits.get(role, {}).get("returncode") == 0, "PROCESS_EXIT_CODE", role)
    if transport(config) == "mtls":
        security = result.get("security", {})
        check(security.get("credentials_deleted") is True and security.get("policy_unchanged") is True,
              "TLS_FIXTURE_CLEANUP")
        check(security.get("profile_inputs_unchanged") is True, "TLS_INPUT_CHANGED")
        routers = security.get("routers", {})
        check(bool(routers) and all(value.get("ready", {}).get("locators") and
              all(locator.startswith("tls/") for locator in value["ready"]["locators"]) and
              value.get("port_closed") is True and
              (value.get("crashed") is True or value.get("session_closed") is True)
              for value in routers.values()), "NATIVE_TLS_EVIDENCE_MISSING")
    check(result.get("disk_bytes", math.inf) <= config["storage"]["maximum_disk_bytes"], "DISK_BUDGET")
    # A local run, even a 24h one, is only one evidence item for C20.
    return {"status": "failed" if failures else {"smoke": "smoke_passed", "baseline": "baseline_round_passed", "soak": "soak_passed"}[config["mode"]],
            "baseline_passed": False,
            "continuous_24h_passed": not failures and config["mode"] == "soak",
            "c20_acceptance_passed": False, "failures": failures,
            "topic_gap_fractions": loss,
            "unknown_thresholds": [key for key, value in thresholds.items() if value is None],
            "limitations": ["owned local loopback " + transport(config) + "; no cross-machine or real-Agent acceptance",
                            "Action internal caches and waiter counts are not exposed by this probe",
                            "Topic gap fractions include declared fault windows; no loss classification is inferred",
                            "recovery proves new-epoch business traffic and exact request-ID duplicate detection",
                            "short-run growth estimates are diagnostic and cannot freeze a 24h resource threshold"]}


def wait_until(function, timeout, code, detail, *, check=None):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check is not None:
            check()
        value = function()
        if value:
            return value
        time.sleep(.05)
    raise ProbeError(code, detail)


def tcp_ready(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=.1):
            return True
    except OSError:
        return False


def existing_json(path):
    return load_json(path) if Path(path).is_file() else None


def jsonl_events(path):
    if not Path(path).is_file():
        return []
    events = []
    for line in Path(path).read_text(errors="replace").splitlines():
        try:
            value = json.loads(line)
        except ValueError:
            continue
        if isinstance(value, dict):
            events.append(value)
    return events


def git_source():
    def run(*args):
        return subprocess.run(["git", "-C", str(ROOT), *args], capture_output=True, text=True,
                              check=True, timeout=5).stdout.strip()
    return {"sha": run("rev-parse", "HEAD"), "status": run("status", "--porcelain"),
            "script_path": str(SCRIPT), "script_sha256": digest(SCRIPT)}


def hardware_info():
    info = {"system": platform.system(), "machine": platform.machine(), "logical_cpus": os.cpu_count(), "hostname": socket.gethostname(),
            "processor": platform.processor() or None, "physical_memory_bytes": None, "unavailable": []}
    try:
        if sys.platform == "darwin":
            for field, key in (("processor", "machdep.cpu.brand_string"), ("physical_memory_bytes", "hw.memsize")):
                value = subprocess.run(["sysctl", "-n", key], capture_output=True, text=True, check=True, timeout=3).stdout.strip()
                info[field] = int(value) if field.endswith("bytes") else value
        elif sys.platform.startswith("linux"):
            info["physical_memory_bytes"] = os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
            for line in Path("/proc/cpuinfo").read_text().splitlines():
                if line.startswith("model name"):
                    info["processor"] = line.partition(":")[2].strip()
                    break
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        info["unavailable"].append(type(error).__name__)
    for name in ("processor", "physical_memory_bytes"):
        if info[name] is None:
            info["unavailable"].append(name + ":not_reported_by_host")
    return info


def blocking_readiness(client):
    return client.wait_for_service(None)


def boundary(args):
    """Observe a blocked public native-wait frame before signaling or closing."""
    import aos_sdk as aos
    output = args.output.resolve()
    manifest = load_json(output / "run.json")
    context = open_context(aos, manifest, "client")
    node = aos.Node("boundary_" + args.kind, manifest["namespace"], context=context)
    client = node.create_client(aos.Interface.load(DEFINITIONS["service"]), "absent_" + args.kind)
    done = threading.Event()
    observed = threading.Event()
    record = {"kind": args.kind, "pid": os.getpid(), "passed": False}
    main_id = threading.get_ident()

    def observe():
        stable_at = None
        while not done.wait(.01):
            frame = sys._current_frames().get(main_id)
            waiting = False
            while frame is not None:
                if frame.f_code is blocking_readiness.__code__:
                    waiting = True
                    break
                frame = frame.f_back
            stable_at = time.monotonic() if waiting and stable_at is None else stable_at if waiting else None
            if stable_at is not None and time.monotonic() - stable_at >= .15:
                record["observed_wait_seconds"] = time.monotonic() - stable_at
                record["observer_ran_during_wait"] = True
                write_json(output / ("boundary-" + args.kind + "-ready.json"), record)
                observed.set()
                if args.kind == "context_close":
                    record["close_started_ns"] = time.monotonic_ns()
                    context.shutdown()
                return

    watcher = threading.Thread(target=observe, name="boundary-observer", daemon=True)
    watcher.start()
    started = time.monotonic()
    try:
        value = blocking_readiness(client)
        record["wait_returned"] = value
        record["outcome"] = "returned"
    except BaseException as error:
        record["outcome"] = type(error).__name__
    finally:
        done.set()
        watcher.join(2)
        if context.ok():
            context.shutdown()
        with contextlib.suppress(aos.InvalidStateError):
            node.destroy_node()
        record["seconds"] = time.monotonic() - started
        record["context_closed"] = not context.ok()
        if "close_started_ns" in record:
            record["close_to_return_seconds"] = (time.monotonic_ns() - record["close_started_ns"]) / 1e9
        record["passed"] = observed.is_set() and not watcher.is_alive() and record["context_closed"] and (
            args.kind == "sigint" and record["outcome"] == "KeyboardInterrupt" or
            args.kind == "context_close" and record["outcome"] in ("InvalidStateError", "returned") and
            record.get("wait_returned") is not True and record.get("close_to_return_seconds", math.inf) < 2)
        write_json(output / ("boundary-" + args.kind + "-final.json"), record)
    return 0 if record["passed"] else 1


class Supervisor:
    def __init__(self, args):
        self.args = args
        self.config = validate_config(load_json(args.config))
        self.binding = verify_artifacts(args.provenance, args.source_sha, args.cli, args.router)
        self.freeze_verification = verify_freeze(self.config, self.binding)
        self.harness_source = git_source()
        if self.config["mode"] in ("baseline", "soak"):
            require(not self.harness_source["status"], "HARNESS_DIRTY", "formal harness source must be committed")
        self.output = args.output.resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        require(shutil.disk_usage(self.output).free >= self.config["storage"]["maximum_disk_bytes"],
                "DISK_FREE_SPACE", "available disk cannot cover the declared output budget")
        self.phase = "preparing"
        self.interrupted = False
        self.processes = {}
        self.exit_records = {}
        self.failures = []
        self.router_number = 0
        self.credentials = None
        self.security = None
        with socket.socket() as port_socket:
            port_socket.bind(("127.0.0.1", 0))
            self.port = port_socket.getsockname()[1]
        self.router_config = self.output / "router.json5"
        self.session_config = self.output / "session.json5"
        write_json(self.router_config, {"mode": "router", "listen": {"endpoints": [f"tcp/127.0.0.1:{self.port}"]},
                   "connect": {"endpoints": []}, "scouting": {"multicast": {"enabled": False}, "gossip": {"enabled": False}}})
        write_json(self.session_config, {"mode": "client", "connect": {
                   "endpoints": [f"tcp/127.0.0.1:{self.port}"], "timeout_ms": 3000, "exit_on_failure": True},
                   "listen": {"endpoints": []}, "scouting": {"multicast": {"enabled": False}, "gossip": {"enabled": False}},
                   "timestamping": {"enabled": {"router": True, "peer": True, "client": True}}})
        self.control = {"phase": self.phase, "epoch": 0, "stop_provider": False, "stop_client": False, "faults": []}
        self.save_control()
        self.manifest = {"protocol": PROTOCOL, "source_sha": args.source_sha, "binding": self.binding,
                         "harness_source": self.harness_source, "harness_sha256": digest(SCRIPT),
                         "run_id": uuid.uuid4().hex, "namespace": "/soak_" + uuid.uuid4().hex[:12],
                         "config": self.config, "input_config_sha256": digest(args.config),
                         "started_unix_ns": time.time_ns(), "platform": platform.platform(),
                         "python": sys.version, "python_executable": sys.executable,
                         "session_config": str(self.session_config), "router_config": str(self.router_config),
                         "topology": "owned local router; client-only SDK sessions; loopback TCP; multicast/gossip disabled",
                         "hardware": hardware_info(),
                         "scope": local_scope_identity(),
                         "configured_client_threads": configured_client_threads(self.config),
                         "inflight_method": "client call/request and provider callback/execute entry/exit counters, plus measurement-phase observations; no artificial business delay",
                         "freeze_verification": self.freeze_verification,
                         "interfaces": DEFINITIONS,
                         "payload_method": "exact compact UTF-8 JSON bytes including workload envelope; transport/schema overhead reported separately as unknown",
                         "transport_overhead_bytes": None,
                         "disk": {"path": str(self.output), "free_bytes_before": shutil.disk_usage(self.output).free}}
        if transport(self.config) == "mtls":
            self.credentials = tempfile.TemporaryDirectory(prefix="aos-c20-pki-")
            try:
                self.security = prepare_tls(self.config, self.manifest["namespace"], self.port,
                                            self.output, Path(self.credentials.name))
            except BaseException:
                self.credentials.cleanup()
                raise
            self.security["routers"] = {}
            self.manifest["security"] = self.security
            self.manifest["session_config"] = None
            self.manifest["router_config"] = None
            self.manifest["topology"] = "owned local native security serve router; mTLS client-only sessions; loopback; exact default-deny ACL; no plaintext fallback"
            self.session_config.unlink()
            self.router_config.unlink()
        write_json(self.output / "run.json", self.manifest)
        self.sampler = ResourceSampler(self.config, self.output,
                                      lambda: [("supervisor", None), *list(self.processes.items())], lambda: self.phase)
        self.sampler.thread.start()
        self.started_measurement = None
        self.measured_seconds = 0
        self.warmed_seconds = 0
        self.fault_results = []
        self.boundary_results = {}
        self.previous_handlers = {}
        self.seen_status = {}
        self.disk_checked_at = 0

    def save_control(self):
        self.control["phase"] = self.phase
        write_json(self.output / "control.json", self.control)

    def start_process(self, name, command):
        process = OwnedProcess(name, command, self.output, self.config["storage"]["logs_bytes_per_process"],
                               environment=clean_environment())
        self.processes[name] = process
        write_json(self.output / "commands.json", {name: {"argv": item.command, "pid": item.process.pid}
                                                   for name, item in self.processes.items()})
        return process

    def start_router(self):
        name = "router_" + str(self.router_number)
        self.router_number += 1
        require(not tcp_ready(self.port), "ROUTER_PORT_BUSY", "selected loopback port is no longer free")
        if self.security is not None:
            self.router = self.start_process(name, [self.args.cli, "security", "serve", "--request", self.security["request"]])
            def ready():
                for item in jsonl_events(self.router.log):
                    if item.get("event") == "router_ready":
                        return item.get("data")
            observed = wait_until(ready, self.config["startup_timeout_seconds"], "ROUTER_START",
                                  "native security service did not report readiness", check=self.check)
            require(observed.get("locators") and all(value.startswith("tls/") for value in observed["locators"]),
                    "TLS_LISTENER", "security service reported a non-TLS listener")
            self.security["routers"][name] = {"ready": observed, "pid": self.router.process.pid}
        else:
            self.router = self.start_process(name, [self.args.router, "-c", self.router_config])
            wait_until(lambda: tcp_ready(self.port), 10, "ROUTER_START", "owned router did not listen", check=self.check)

    def stop_process(self, name, timeout=5, signum=signal.SIGTERM):
        self.exit_records[name] = self.processes[name].stop(timeout, signum)
        if self.security is not None and name in self.security["routers"]:
            record = self.security["routers"][name]
            code = self.exit_records[name]["returncode"]
            record.update(crashed=signum == signal.SIGKILL and code == -signal.SIGKILL,
                          port_closed=not tcp_ready(self.port), returncode=code, session_closed=False)
            summaries = [item.get("data", {}) for item in jsonl_events(self.processes[name].log) if item.get("event") == "summary"]
            if summaries:
                record["session_closed"] = code == 143 and summaries[-1].get("session_closed") is True and summaries[-1].get("cleanup_error") is None
        return self.exit_records[name]

    def check(self):
        require(not self.interrupted, "INTERRUPTED", "run was interrupted; a new run is required")
        for name, process in list(self.processes.items()):
            require(process.expected_exit or process.process.poll() is None, "PROCESS_EXITED", name + " exited unexpectedly")
        require(not self.sampler.failures, "RESOURCE_SAMPLING_FAILED", "resource sampler has failed")
        if time.monotonic() - self.disk_checked_at >= 5:
            used = sum(path.stat().st_size for path in self.output.rglob("*") if path.is_file())
            require(used <= self.config["storage"]["maximum_disk_bytes"], "DISK_BUDGET", "run exceeded its disk budget")
            self.disk_checked_at = time.monotonic()
        for role in ("provider", "client"):
            state = existing_json(self.output / (role + "-status.json"))
            if not state or self.seen_status.get(role) == state["unix_ns"]:
                continue
            self.seen_status[role] = state["unix_ns"]
            require(state["counters"].get("unexpected_errors", 0) == 0, "WORKER_ERROR", role + " reported an unexpected error")
            for name, stats in state["queues"].items():
                if stats is None and self.phase != "measuring":
                    continue  # A Client has no reply queue until its first call.
                errors = queue_violations(role + ":" + name, stats, self.config["thresholds"]["require_queue_byte_stats"])
                require(not errors, "QUEUE_BOUND", ", ".join(errors))
            if "bag" in state:
                bag = state["bag"]
                limits = self.config["workload"]["bag"]
                require(0 <= bag["queue_bytes"] <= limits["queue_bytes"] and
                        0 <= bag["queue_messages"] <= limits["queue_messages"], "BAG_QUEUE_BOUND", "bag queue exceeded its budget")
            now_ns = time.time_ns()
            in_fault = any(item["started_unix_ns"] <= now_ns <= item["deadline_unix_ns"] for item in self.control["faults"])
            if self.phase == "measuring" and role == "client" and not in_fault:
                limit = self.config["thresholds"]["maximum_business_silence_seconds"]
                if limit is not None:
                    for key in business_keys(self.config):
                        last = state["last_success_unix_ns"].get(key, now_ns)
                        require((now_ns - last) / 1e9 <= limit, "BUSINESS_SILENCE", key)

    def pause(self, seconds):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            self.check()
            time.sleep(min(.1, max(0, deadline - time.monotonic())))

    def fault(self, specification):
        # Mark the expected window before killing only our owned router group.
        begin = time.monotonic()
        deadline_ns = time.time_ns() + int((specification["down_seconds"] + specification["recovery_timeout_seconds"]) * 1e9)
        event = {**specification, "started_unix_ns": time.time_ns(), "deadline_unix_ns": deadline_ns,
                 "epoch": self.control["epoch"] + 1, "recovered": False}
        self.control["faults"].append(event)
        self.save_control()
        self.fault_results.append(event)
        killed = self.stop_process(self.router.name, 5, signal.SIGKILL)
        event["old_pid"] = killed["pid"]
        event["old_process_reaped"] = killed["reaped"]
        event["port_closed"] = not tcp_ready(self.port)
        require(event["port_closed"], "ROUTER_FAULT_NOT_ISOLATED", "router port stayed open after its owned process exited")
        self.pause(specification["down_seconds"])
        self.start_router()
        restart = time.monotonic()
        event["new_pid"] = self.router.process.pid
        self.control["epoch"] = event["epoch"]
        self.save_control()
        wait_until(lambda: epoch_recovered(self.config, existing_json(self.output / "client-status.json") or {}, event["epoch"]),
                   specification["recovery_timeout_seconds"], "ROUTER_RECOVERY_TIMEOUT",
                   "new-epoch Topic, Service, Action success and cancel did not all recover", check=self.check)
        event.update(recovered=True, recovery_seconds=time.monotonic() - restart,
                     total_fault_seconds=time.monotonic() - begin, recovered_unix_ns=time.time_ns())
        self.save_control()
        write_json(self.output / "faults.json", self.fault_results)

    def run_boundary(self, kind):
        name = "boundary_" + kind
        process = self.start_process(name, [sys.executable, "-I", SCRIPT, "_boundary", "--output", self.output, "--kind", kind])
        ready = self.output / ("boundary-" + kind + "-ready.json")
        # context_close may finish immediately after writing readiness.
        observation = wait_until(lambda: existing_json(ready), self.config["startup_timeout_seconds"],
                                 "BOUNDARY_NOT_WAITING", kind + " did not enter its native readiness wait")
        if kind == "sigint":
            sent = time.monotonic()
            os.kill(process.process.pid, signal.SIGINT)
        else:
            sent = time.monotonic()
        timed_out = False
        try:
            process.process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            timed_out = True
        self.exit_records[name] = process.stop()
        record = existing_json(self.output / ("boundary-" + kind + "-final.json")) or {"kind": kind, "passed": False}
        record.update(wait_observation=observation, completion_timed_out=timed_out,
                      returncode=process.process.returncode)
        record["supervisor_completion_seconds"] = time.monotonic() - sent
        record["passed"] = record.get("passed") is True and process.process.returncode == 0 and record["supervisor_completion_seconds"] < 3
        self.boundary_results[kind] = record
        require(record["passed"], "BOUNDARY_FAILED", kind + " failed")

    def graceful_worker(self, role):
        if role not in self.processes:
            return
        self.control["stop_" + role] = True
        self.save_control()
        process = self.processes[role]
        process.expected_exit = True
        try:
            process.process.wait(timeout=self.config["shutdown_timeout_seconds"] + self.config["quiet_seconds"] + 5)
        except subprocess.TimeoutExpired:
            self.failures.append({"code": "WORKER_SHUTDOWN_TIMEOUT", "detail": role})
        self.exit_records[role] = process.stop(2)
        if process.process.returncode != 0:
            self.failures.append({"code": "WORKER_EXIT_CODE", "detail": role, "returncode": process.process.returncode})

    def run(self):
        finished = False
        result = None
        try:
            for signum in (signal.SIGINT, signal.SIGTERM):
                self.previous_handlers[signum] = signal.getsignal(signum)
                signal.signal(signum, lambda *_: setattr(self, "interrupted", True))
            self.start_router()
            for role in ("provider", "client"):
                self.start_process(role, [sys.executable, "-I", SCRIPT, "_worker", "--role", role, "--output", self.output])
                wait_until(lambda role=role: existing_json(self.output / (role + "-ready.json")),
                           self.config["startup_timeout_seconds"], "WORKER_READINESS", role + " did not initialize", check=self.check)
            self.phase = "warming"
            self.save_control()
            wait_until(lambda: epoch_recovered(self.config, existing_json(self.output / "client-status.json") or {}, 0),
                       self.config["startup_timeout_seconds"], "BUSINESS_READINESS", "initial business paths did not all succeed", check=self.check)
            warm_start = time.monotonic()
            self.pause(self.config["warmup_seconds"])
            self.warmed_seconds = time.monotonic() - warm_start
            self.phase = "measuring"
            self.save_control()
            self.started_measurement = time.monotonic()
            for fault in self.config["faults"]:
                self.pause(max(0, self.started_measurement + fault["at_seconds"] - time.monotonic()))
                self.fault(fault)
            self.pause(max(0, self.started_measurement + self.config["duration_seconds"] - time.monotonic()))
            self.measured_seconds = time.monotonic() - self.started_measurement
            self.phase = "draining"
            self.save_control()
            finished = True
        except BaseException as error:
            self.failures.append({"code": getattr(error, "code", "HARNESS_ERROR"),
                                  "type": type(error).__name__, "message": str(error)[:1000]})
            if self.started_measurement is not None:
                self.measured_seconds = time.monotonic() - self.started_measurement
        finally:
            self.phase = "draining"
            with contextlib.suppress(Exception):
                self.save_control()
            for role in ("client", "provider"):
                try:
                    self.graceful_worker(role)
                except BaseException as error:
                    self.failures.append({"code": "WORKER_CLEANUP", "type": type(error).__name__, "detail": role})
            if finished and not self.interrupted:
                for kind in ("sigint", "context_close"):
                    try:
                        self.run_boundary(kind)
                    except BaseException as error:
                        self.failures.append({"code": getattr(error, "code", "BOUNDARY_FAILED"), "type": type(error).__name__, "detail": kind})
            for name, process in list(self.processes.items()):
                if name not in self.exit_records:
                    try:
                        self.stop_process(name, timeout=10)
                    except BaseException as error:
                        self.failures.append({"code": "PROCESS_CLEANUP", "type": type(error).__name__, "detail": name})
            if self.security is not None:
                try:
                    self.security["policy_unchanged"] = (
                        digest(self.output / "security-policy.json") == self.security["policy_sha256"] and
                        digest(self.output / "compiled-security-policy.json") == self.security["compiled_policy_sha256"])
                    self.security["profile_inputs_unchanged"] = all(
                        digest(item["path"]) == item["sha256"] for item in self.security["input_files"].values())
                except OSError:
                    self.security["policy_unchanged"] = False
                    self.security["profile_inputs_unchanged"] = False
                finally:
                    credential_path = Path(self.credentials.name)
                    self.credentials.cleanup()
                    self.security["credentials_deleted"] = not credential_path.exists()
            resources = self.sampler.finish()
            self.phase = "stopped"
            self.save_control()
            binding_after = None
            freeze_after = None
            try:
                binding_after = verify_artifacts(self.args.provenance, self.args.source_sha, self.args.cli, self.args.router)
                freeze_after = verify_freeze(self.config, binding_after)
            except BaseException as error:
                self.failures.append({"code": getattr(error, "code", "ARTIFACT_CHANGED"), "type": type(error).__name__})
            result = {**self.manifest, "finished_normally": finished, "interrupted": self.interrupted,
                      "warmup_seconds": self.warmed_seconds,
                      "measurement_seconds": self.measured_seconds, "finished_unix_ns": time.time_ns(),
                      "binding_after": binding_after, "harness_after": digest(SCRIPT),
                      "freeze_after": freeze_after,
                      "config_after": digest(self.args.config),
                      "failures": self.failures, "resources": resources, "faults": self.fault_results,
                      "workers": {role: existing_json(self.output / (role + "-final.json")) or {}
                                  for role in ("provider", "client")},
                      "bag": existing_json(self.output / "bag-final.json"),
                      "boundaries": self.boundary_results, "process_exits": self.exit_records,
                      "disk_bytes": sum(path.stat().st_size for path in self.output.rglob("*") if path.is_file())}
            result["bag"] = result["bag"] or {}
            result["measured_rates"] = {
                role: {key + "_per_second": count / self.measured_seconds
                       for key, count in worker.get("measurement_counters", {}).items()}
                for role, worker in result["workers"].items()
            } if self.measured_seconds > 0 else {}
            try:
                result["raw_evidence"] = seal_raw_evidence(self.output, result["run_id"])
                verify_raw_evidence(self.output, result)
                result["disk_bytes"] = sum(path.stat().st_size for path in self.output.rglob("*") if path.is_file())
            except BaseException as error:
                self.failures.append({"code": getattr(error, "code", "RAW_EVIDENCE_SEAL"), "type": type(error).__name__})
            result["gate"] = evaluate(self.config, result)
            write_json(self.output / "result.json", result)
            for signum, handler in self.previous_handlers.items():
                signal.signal(signum, handler)
        print(json.dumps({"status": result["gate"]["status"], "continuous_24h_passed": result["gate"]["continuous_24h_passed"],
                          "result": str(self.output / "result.json"), "failure_codes": [item["code"] for item in result["gate"]["failures"]]}))
        return 0 if result["gate"]["status"] != "failed" else 1


def baseline_gate(config, rounds):
    """Single-round or mixed-candidate evidence can never claim baseline_passed."""
    validate_config(config)
    failures = []
    if config["mode"] != "baseline" or len(rounds) != config.get("rounds") or len(rounds) < 3:
        failures.append({"code": "BASELINE_ROUNDS"})
    if rounds:
        first = rounds[0]
        for index, value in enumerate(rounds):
            if value.get("config") != config:
                failures.append({"code": "BASELINE_CONFIG_CHANGED", "round": index + 1})
            if any(value.get(key) != first.get(key) for key in
                   ("binding", "harness_sha256", "harness_source", "input_config_sha256", "platform", "hardware")):
                failures.append({"code": "BASELINE_CANDIDATE_CHANGED", "round": index + 1})
            try:
                round_scope(value)
                check = evaluate(validate_config(value["config"]), value)
                if check["status"] != "baseline_round_passed":
                    failures.append({"code": "BASELINE_ROUND_FAILED", "round": index + 1, "failures": check["failures"]})
            except (ProbeError, KeyError, TypeError) as error:
                failures.append({"code": "BASELINE_ROUND_INVALID", "round": index + 1, "type": type(error).__name__})
        ids = [value.get("run_id") for value in rounds]
        if not all(ids) or len(set(ids)) != len(rounds):
            failures.append({"code": "BASELINE_DUPLICATE_ROUND"})
    return {"status": "failed" if failures else "baseline_passed", "baseline_passed": not failures,
            "continuous_24h_passed": False, "c20_acceptance_passed": False, "failures": failures,
            "scope": "one local hardware, transport and workload case; other matrix cells are independent"}


def summarize_rounds(rounds):
    """Preserve every round; across-round ranges never average percentiles."""
    measured = []
    for index, value in enumerate(rounds):
        client = value.get("workers", {}).get("client", {})
        measured.append({"round": index + 1, "run_id": value.get("run_id"),
            "warmup_seconds": value.get("warmup_seconds"), "measurement_seconds": value.get("measurement_seconds"),
            "latency_seconds": client.get("measurement_latencies", {}), "rates": value.get("measured_rates", {}),
            "counts": {role: worker.get("measurement_counters", {}) for role, worker in value.get("workers", {}).items()},
            "topic_sequence_gap_fractions": evaluate(value["config"], value)["topic_gap_fractions"],
            "resource_peaks": value.get("resources", {}).get("peaks", {}),
            "resource_growth_per_hour": {name: report.get("growth_per_hour") for name, report in value.get("resources", {}).get("trends", {}).items()},
            "cpu_percent": value.get("resources", {}).get("cpu_percent", {}),
            "sampled_queue_peaks": {role: worker.get("sampled_queue_peaks", {}) for role, worker in value.get("workers", {}).items()}})
        measured[-1]["configured_client_threads"] = value.get("configured_client_threads")
        measured[-1]["inflight"] = {role: worker.get("inflight", {}) for role, worker in value.get("workers", {}).items()}
    numbers = {}

    def flatten(value, path, index):
        if isinstance(value, dict):
            for key, item in value.items():
                flatten(item, path + "/" + key, index)
        elif type(value) in (int, float) and math.isfinite(value):
            numbers.setdefault(path, {})[index] = value

    for index, value in enumerate(measured):
        for name in ("latency_seconds", "rates", "topic_sequence_gap_fractions", "resource_peaks", "resource_growth_per_hour", "cpu_percent", "sampled_queue_peaks", "inflight"):
            flatten(value[name], name, index)
    ranges = {name: {"available_rounds": len(values), "minimum": min(values.values()),
                     "median": statistics.median(values.values()), "maximum": max(values.values())}
              for name, values in numbers.items()}
    return {"measured_rounds": measured, "across_round_ranges": ranges,
            "method": "retain all rounds; min/median/max of each reported statistic across rounds; no pooled p99 or best-round selection",
            "threshold_proposal": {"status": "not_frozen", "values": None,
                "method": "review worst-round control latency, loss and resource peaks together with raw samples; choose explicit headroom and growth limits in a separate freeze change",
                "reason": "one local baseline cannot set limits for unmeasured hardware/transports or prove a 24h resource trend"}}


def run_baseline(args):
    config = validate_config(load_json(args.config))
    require(config["mode"] == "baseline", "BASELINE_MODE", "baseline coordinator requires baseline config")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    rounds, evidence = [], []
    for index in range(config["rounds"]):
        current = argparse.Namespace(**vars(args))
        current.output = output / f"round-{index + 1:02d}"
        code = Supervisor(current).run()
        result_path = current.output / "result.json"
        value = load_verified_run(result_path)
        rounds.append(value)
        evidence.append({"path": str(result_path.relative_to(output)), "sha256": digest(result_path)})
        write_json(output / "progress.json", {"completed_rounds": len(rounds), "required_rounds": config["rounds"],
                                             "round_evidence": evidence, "baseline_passed": False})
        if code:
            break
    result = {"protocol": PROTOCOL, "config": config, "gate": baseline_gate(config, rounds),
              "binding": rounds[0]["binding"] if rounds else None, "round_evidence": evidence,
              "scope": rounds[0].get("scope") if rounds else None,
              "harness_sha256": rounds[0].get("harness_sha256") if rounds else None,
              "summary": summarize_rounds(rounds), "continuous_24h_passed": False,
              "limitations": ["local baseline only; no cross-machine, real-Agent or 24h acceptance", "threshold proposal is separate and is not frozen"]}
    write_json(output / "baseline-result.json", result)
    print(json.dumps({"status": result["gate"]["status"], "baseline_passed": result["gate"]["baseline_passed"],
                      "continuous_24h_passed": False, "result": str(output / "baseline-result.json")}))
    return 0 if result["gate"]["baseline_passed"] else 1


def evaluate_baseline(path):
    result = load_json(path)
    rounds = []
    for item in result["round_evidence"]:
        relative = Path(item["path"])
        require(not relative.is_absolute() and ".." not in relative.parts, "BASELINE_PATH", "round evidence must stay within its baseline directory")
        source = Path(path).parent / relative
        require(digest(source) == item["sha256"], "BASELINE_MISMATCH", "round evidence bytes changed")
        rounds.append(load_verified_run(source))
    gate = baseline_gate(validate_config(result["config"]), rounds)
    if not rounds or result.get("binding") != rounds[0].get("binding"):
        gate["failures"].append({"code": "BASELINE_BINDING_CHANGED"})
        gate.update(status="failed", baseline_passed=False)
    if rounds and (result.get("scope") != rounds[0].get("scope") or
                   result.get("harness_sha256") != rounds[0].get("harness_sha256")):
        gate["failures"].append({"code": "BASELINE_SCOPE_MISMATCH"})
        gate.update(status="failed", baseline_passed=False)
    return gate, summarize_rounds(rounds)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("run", help="run a new owned local smoke, >=3-round baseline or separately frozen soak")
    for name in ("config", "provenance", "cli", "router", "output"):
        run.add_argument("--" + name, type=Path, required=True)
    run.add_argument("--source-sha", required=True)
    config = subparsers.add_parser("check-config", help="reject unfrozen formal configuration without starting processes")
    config.add_argument("--config", type=Path, required=True)
    offline = subparsers.add_parser("evaluate", help="re-evaluate saved evidence without changing it")
    offline.add_argument("--result", type=Path, required=True)
    summary = subparsers.add_parser("summarize", help="read all baseline rounds with hashes and separate measured values from threshold proposals")
    summary.add_argument("--result", type=Path, required=True)
    worker = subparsers.add_parser("_worker")
    worker.add_argument("--output", type=Path, required=True)
    worker.add_argument("--role", choices=("provider", "client"), required=True)
    helper = subparsers.add_parser("_boundary")
    helper.add_argument("--output", type=Path, required=True)
    helper.add_argument("--kind", choices=("sigint", "context_close"), required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "check-config":
            validated = validate_config(load_json(args.config))
            print(json.dumps({"status": "valid", "mode": validated["mode"], "starts_processes": False}))
            return 0
        if args.command == "summarize":
            gate, summary = evaluate_baseline(args.result)
            print(json.dumps({"gate": gate, "summary": summary}, indent=2))
            return 0 if gate["baseline_passed"] else 1
        if args.command == "evaluate":
            result = load_json(args.result)
            if "round_evidence" in result:
                gate = evaluate_baseline(args.result)[0]
            else:
                result = load_verified_run(args.result)
                gate = evaluate(validate_config(result["config"]), result)
            print(json.dumps(gate, indent=2))
            return 0 if gate["status"] != "failed" else 1
        if args.command == "_worker":
            return Worker(args).run()
        if args.command == "_boundary":
            return boundary(args)
        if load_json(args.config).get("mode") == "baseline":
            return run_baseline(args)
        return Supervisor(args).run()
    except BaseException as error:
        print(json.dumps({"status": "failed", "code": getattr(error, "code", "HARNESS_ERROR"),
                          "type": type(error).__name__, "message": str(error)[:1000]}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
