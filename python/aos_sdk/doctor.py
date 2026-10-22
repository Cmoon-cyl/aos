"""Bounded installation and discovery diagnostics, using only the standard library.

Run ``python -m aos_sdk.doctor --help`` after a successful SDK import. If the
native import is broken, run this file directly with the affected interpreter.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import importlib.machinery
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import re
import stat
import subprocess
import sys
import tempfile
import time
from typing import Any

__all__ = ["doctor_report"]

_FRAME = "AOS_DOCTOR_REPORT "
_PACKAGE_DIR = Path(__file__).absolute().parent


def _timeout(value: Any) -> float:
    try:
        seconds = float(value)
    except (TypeError, ValueError, OverflowError) as error:
        raise ValueError("timeout must be a number in (0, 86400] seconds") from error
    if isinstance(value, bool) or not math.isfinite(seconds) or not 0 < seconds <= 86400:
        raise ValueError("timeout must be a number in (0, 86400] seconds")
    return seconds


def _report(options: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "status": "running",
        "timeout_seconds": options["timeout"],
        "elapsed_seconds": None,
        "runtime": {
            "python_version": platform.python_version(),
            "python_implementation": sys.implementation.name,
            "executable": sys.executable,
            "platform": sys.platform,
        },
        "sdk": {"distribution_version": None, "native_version": None},
        "native": {
            "status": "not_run", "available": None,
            "module": "aos_sdk._native", "path": None,
        },
        "config": {
            "status": "not_run", "source": options["config_source"],
            "path": options["config"], "exists": None,
            "size_bytes": None, "sha256": None,
        },
        "domain": {
            "status": "not_run", "source": options["domain_source"], "value": None,
        },
        "context": {
            "status": "not_run", "ok": None,
            "scope": "Context.ok() reports local shutdown state, not link health.",
        },
        "deployment": {
            "process_config_present": options.get("process_config_present", False),
            "used_by_probe": False,
            "validation": "not_run",
            "scope": "The isolated diagnostic Context excludes AOS_PROCESS_CONFIG. "
                     "This report does not validate the launch child's bootstrap or effective configuration.",
        },
        "router": {
            "status": "unknown", "ids": None,
            "reason": "The Python Context API does not expose connected router IDs. "
                      "Opening a session or observing Graph entities does not prove a router connection.",
        },
        "graph": {
            "status": "not_run", "entity_count": None, "timeout_seconds": None,
            "scope": "One bounded discovery snapshot; an empty result does not prove no peers exist.",
        },
        "telemetry": {
            "status": "not_run", "snapshot": None,
            "scope": "This diagnostic subprocess's Context only; not a remote or aggregate view.",
        },
        "shutdown": {"status": "not_run"},
        "security": {
            "profile_selected": options.get("security_profile") is not None,
            "profile_path": None,
            "authentication": "unknown", "authorization": "unknown", "encryption": "unknown",
            "reason": "No identity, access-control or encryption verification is performed. "
                      "A domain ID is a logical namespace, not authentication.",
        },
        "probe": {"stage": "startup", "returncode": None},
        "cleanup": {"status": "not_run", "forced": False, "reaped": False},
        "failures": [],
    }


def _failure(report, stage, code, hint, error=None):
    failure = {"stage": stage, "code": code, "hint": hint}
    if error is not None:
        # Config/parser/transport exceptions can contain credentials or file contents.
        failure["exception_type"] = type(error).__name__
    report["failures"].append(failure)
    report["status"] = "error"
    if stage in report and isinstance(report[stage], dict):
        report[stage]["status"] = "error"


def _emit(report, stage):
    report["probe"]["stage"] = stage
    print(_FRAME + json.dumps(report, ensure_ascii=True, allow_nan=False), flush=True)


def _config_and_domain(report, options):
    config = report["config"]
    if options["config"] is None:
        config["status"] = "ok"
        config["summary"] = (
            "Native TLS configuration generated from the explicit security profile."
            if options.get("security_profile") is not None
            else "Embedded native configuration; its bytes are not exposed by the SDK."
        )
    else:
        try:
            file_stat = os.stat(options["config"])
            config["exists"] = True
            if not stat.S_ISREG(file_stat.st_mode):
                _failure(report, "config", "CONFIG_NOT_FILE", "Choose a regular Zenoh session config file.")
            else:
                digest = hashlib.sha256()
                size = 0
                with open(options["config"], "rb") as source:
                    for block in iter(lambda: source.read(64 * 1024), b""):
                        digest.update(block)
                        size += len(block)
                config.update(status="ok", size_bytes=size, sha256=digest.hexdigest())
                config["summary"] = "File bytes hashed; content is omitted. Native Context validates the config."
        except FileNotFoundError as error:
            config["exists"] = False
            _failure(report, "config", "CONFIG_NOT_FOUND", "Check the selected config path.", error)
        except (OSError, ValueError) as error:
            _failure(report, "config", "CONFIG_UNREADABLE", "Check config file access and permissions.", error)

    value = options["domain_id"]
    if options["domain_source"] == "security_profile":
        report["domain"].update(status="pending", value=None)
        return
    if options["domain_source"] != "argument":
        value = options.get("domain_environment", "") or "0"
        if re.fullmatch(r"\+?[0-9]+", value) and len(value) < 12:
            value = int(value)
    if type(value) is not int or not 0 <= value <= 2**32 - 1:
        _failure(report, "domain", "INVALID_DOMAIN_ID", "Use a domain ID integer in 0..4294967295.")
    else:
        report["domain"].update(status="ok", value=value)


def _native(report):
    details = report["native"]
    try:
        spec = importlib.machinery.PathFinder.find_spec("aos_sdk._native", [str(_PACKAGE_DIR)])
        if spec is not None:
            details["path"] = spec.origin
        native = importlib.import_module("aos_sdk._native")
        details["path"] = native.__file__
        if not isinstance(native.__loader__, importlib.machinery.ExtensionFileLoader):
            _failure(report, "native", "NOT_NATIVE_EXTENSION", "Install the platform's AOS SDK wheel.")
            details["available"] = False
            return None
        details.update(status="ok", available=True)
        report["sdk"]["native_version"] = native.__version__
        version = report["sdk"]["distribution_version"]
        if version is not None and version != native.__version__:
            _failure(report, "native", "VERSION_MISMATCH", "Reinstall matching SDK Python files and native extension.")
        return native
    except Exception as error:
        details["available"] = False
        _failure(
            report, "native", "NATIVE_IMPORT_FAILED",
            "Check the wheel's Python ABI, OS, CPU architecture and shared-library dependencies.", error,
        )
        return None


def _probe(options):
    # The subprocess uses this file's package, including when run as a rescue script.
    sys.path.insert(0, str(_PACKAGE_DIR.parent))
    # Inherit the native environment so os.putenv()/embedding-host selections
    # are not lost when the parent's Python mapping is stale. Resolve the two
    # snapshots before sanitizing deployment identity in this child.
    options = options.copy()
    options["process_config_present"] |= "AOS_PROCESS_CONFIG" in os.environ
    os.environ.pop("AOS_PROCESS_CONFIG", None)
    native_profile = os.environ.pop("AOS_SECURITY_PROFILE", None)
    selection_conflict = False
    if not options["security_profile_explicit"]:
        python_profile = options["security_profile"]
        selection_conflict = (python_profile is not None and native_profile is not None
                              and python_profile != native_profile)
        if python_profile is None:
            options["security_profile"] = native_profile
    if options["security_profile"] is not None:
        if options["config"] is None:
            options["config_source"] = "security_profile"
        if options["domain_source"] == "default":
            options["domain_source"] = "security_profile"
    report = _report(options)
    _emit(report, "runtime")
    if selection_conflict:
        _failure(report, "context", "INVALID_ARGUMENT",
                 "Python and native AOS_SECURITY_PROFILE values disagree.")
        _emit(report, "complete")
        return
    report["runtime"].update(
        machine=platform.machine(), system=platform.system(), release=platform.release(),
    )
    if sys.platform.startswith("linux"):
        name, version = platform.libc_ver()
        report["runtime"]["libc"] = {"name": name or None, "version": version or None}
    try:
        report["sdk"]["distribution_version"] = importlib.metadata.version("aos-sdk")
    except importlib.metadata.PackageNotFoundError:
        pass
    _emit(report, "config")
    _config_and_domain(report, options)
    _emit(report, "native")
    native = _native(report)
    if report["failures"]:
        _emit(report, "complete")
        return

    context = None
    try:
        report["context"]["status"] = "running"
        _emit(report, "context")
        overrides = {"zenoh_config": options["config"]}
        if report["domain"]["value"] is not None:
            overrides["domain_id"] = report["domain"]["value"]
        context = native.Context(
            cli_overrides=overrides,
            security_profile=options.get("security_profile"),
            _environment={},
        )
        report["domain"].update(
            status="ok", value=context.effective_config["domain_id"]["value"],
        )
        if options.get("security_profile") is not None:
            report["security"].update(
                profile_path=options["security_profile"],
                authentication="not independently observed; a peer session may open before its Router connects",
                encryption="TLS-only validated configuration",
                authorization="unknown",
                reason="Session open does not prove connectivity or endpoint grants. "
                       "Verify remote business effects and denial evidence.",
            )
        report["context"]["ok"] = context.ok()
        if not report["context"]["ok"]:
            _failure(report, "context", "CONTEXT_CLOSED", "The newly opened Context is already closed.")
        else:
            report["context"]["status"] = "ok"
            graph_timeout = max(0.0, min(0.2, options["probe_deadline"] - time.monotonic()))
            report["graph"].update(status="running", timeout_seconds=graph_timeout)
            _emit(report, "graph")
            entities = context.graph(timeout=graph_timeout)
            report["graph"].update(status="ok", entity_count=len(entities))
    except Exception as error:
        stage = report["probe"]["stage"]
        if type(error).__name__ == "InvalidArgumentError" and stage == "context":
            code, hint = "CONFIG_INVALID", "The native SDK rejected the selected session config."
        elif type(error).__name__ == "CommunicationError":
            code, hint = "COMMUNICATION_FAILED", "Check the session config, reachable peers/router and network."
        elif type(error).__name__ == "WaitTimeoutError":
            code, hint = "DISCOVERY_TIMEOUT", "Discovery did not finish inside its wait window."
        else:
            code, hint = "CONTEXT_PROBE_FAILED", "The Context or Graph operation failed."
        _failure(report, stage, code, hint, error)
    finally:
        if context is not None:
            report["shutdown"]["status"] = "running"
            _emit(report, "shutdown")
            try:
                context.shutdown()
                report["shutdown"]["status"] = "ok"
            except Exception as error:
                _failure(report, "shutdown", "SHUTDOWN_FAILED", "The diagnostic Context could not close cleanly.", error)
            report["telemetry"]["status"] = "running"
            _emit(report, "telemetry")
            try:
                report["telemetry"].update(status="ok", snapshot=context.telemetry_snapshot())
            except Exception as error:
                _failure(
                    report, "telemetry", "TELEMETRY_FAILED",
                    "The diagnostic Context's local counters could not be read.", error,
                )
    if not report["failures"]:
        report["status"] = "ok"
    _emit(report, "complete")


def _last_report(output, fallback):
    for line in reversed(output.decode("utf-8", errors="replace").splitlines()):
        if line.startswith(_FRAME):
            try:
                result = json.loads(line[len(_FRAME):])
            except ValueError:
                continue
            if isinstance(result, dict) and result.get("schema_version") == 1:
                return result
    return fallback


def _kill_and_reap(process, deadline):
    try:
        process.kill()
        process.wait(timeout=max(0.0, deadline - time.monotonic()))
    except (OSError, subprocess.TimeoutExpired):
        pass


def doctor_report(
    *, config: str | os.PathLike[str] | None = None,
    domain_id: int | None = None,
    security_profile: str | os.PathLike[str] | None = None,
    timeout: float = 5.0,
) -> dict[str, Any]:
    """Return JSON-serializable installation and discovery diagnostics.

    ``timeout`` is the total budget in seconds, including child cleanup.
    It must be finite and in (0, 86400]. The last 20% (at most 0.2 seconds)
    is reserved for forced cleanup. The child creates its own Context and
    only queries Graph and local counters; it never creates a Node or sends
    business requests. The telemetry snapshot belongs to that child Context,
    is taken after shutdown, and is not a remote or aggregate measurement.

    Config selection is explicit ``config``, then nonempty ``AOS_ZENOH_CONFIG``, then
    the native built-in config. The child receives these resolved values as
    final overrides and does not read the configuration environment again.
    Its environment excludes ``AOS_PROCESS_CONFIG`` so it cannot inherit a
    launch child's identity or collide with deployment CLI fields. The report
    records whether that variable was present; deployment validation is not
    performed, and the caller's environment is unchanged.

    Security selection is explicit ``security_profile``, then
    ``AOS_SECURITY_PROFILE`` from Python or the native environment. Conflicting
    environment values fail closed. Empty, relative or invalid selections fail
    without development-mode fallback. With no domain override, the profile
    supplies the domain. A successful TLS handshake verifies the selected
    connection; authorization remains unknown until individual operations
    and router denial evidence have been checked.
    """
    timeout = _timeout(timeout)
    started = time.monotonic()
    deadline = started + timeout
    config_source = "argument"
    if config is None:
        config = os.environ.get("AOS_ZENOH_CONFIG") or None
        config_source = "environment" if config is not None else "built_in"
    if config is not None:
        config = os.fspath(config)
        if not isinstance(config, str):
            raise TypeError("config must be a string path or PathLike[str]")
        config = os.path.abspath(config)
    security_profile_explicit = security_profile is not None
    if security_profile is None:
        security_profile = os.environ.get("AOS_SECURITY_PROFILE")
    if security_profile is not None:
        security_profile = os.fspath(security_profile)
        if not isinstance(security_profile, str):
            raise TypeError("security_profile must be a string path or PathLike[str]")
        # Do not turn empty/relative secure selection into a different profile.
        # The shared native loader rejects those paths without any fallback.
        if config is None:
            config_source = "security_profile"
    options = {
        "timeout": timeout, "probe_deadline": deadline - min(0.2, timeout * 0.2),
        "config": config, "config_source": config_source, "domain_id": domain_id,
        "security_profile": security_profile,
        "security_profile_explicit": security_profile_explicit,
        "domain_environment": os.environ.get("AOS_DOMAIN_ID", ""),
        "domain_source": "argument" if domain_id is not None else (
            "environment" if os.environ.get("AOS_DOMAIN_ID") else (
                "security_profile" if security_profile is not None else "default"
            )
        ),
        "process_config_present": "AOS_PROCESS_CONFIG" in os.environ,
    }
    report = _report(options)
    forced = False
    output = b""
    process = None
    io_error = None
    try:
        # A private temporary file avoids PIPE reader threads (including on
        # Windows) and keeps native stdout/stderr out of the public report.
        with tempfile.TemporaryFile() as progress:
            process = subprocess.Popen(
                [sys.executable, "-I", str(_PACKAGE_DIR / "doctor.py"), "--_probe", json.dumps(options)],
                stdin=subprocess.DEVNULL, stdout=progress, stderr=subprocess.DEVNULL,
                # The child resolves the native/Python security snapshots and
                # removes deployment identity before importing the extension.
                env=None,
            )
            try:
                process.wait(timeout=max(0.0, options["probe_deadline"] - time.monotonic()))
            except subprocess.TimeoutExpired:
                forced = True
                _kill_and_reap(process, deadline)
            progress.seek(0, os.SEEK_END)
            # Our frames are small. Bound memory even if a future native module
            # writes verbose diagnostics before crashing.
            progress.seek(max(0, progress.tell() - 256 * 1024))
            output = progress.read(256 * 1024)
    except OSError as error:
        io_error = error
        if process is None:
            _failure(report, "startup", "PROBE_START_FAILED", "Check interpreter execution and temporary-file access.", error)
            report["elapsed_seconds"] = round(time.monotonic() - started, 6)
            return report
    finally:
        if process is not None and process.poll() is None:
            forced = True
            _kill_and_reap(process, deadline)

    report = _last_report(output, report)
    report["probe"]["returncode"] = process.poll()
    reaped = process.returncode is not None
    report["cleanup"] = {
        "status": "ok" if reaped else "timeout", "forced": forced, "reaped": reaped,
    }
    if io_error is not None:
        _failure(report, "probe", "PROBE_IO_FAILED", "Check temporary-file access for diagnostic output.", io_error)
    if forced:
        stage = report["probe"]["stage"]
        _failure(report, stage, "PROBE_TIMEOUT", "The total diagnostic time budget expired; no business request was sent.")
        report["status"] = "timeout"
        if stage in report and isinstance(report[stage], dict):
            report[stage]["status"] = "timeout"
    elif process.returncode != 0 or report["probe"]["stage"] != "complete":
        _failure(report, report["probe"]["stage"], "PROBE_EXITED", "The diagnostic subprocess exited without a complete report.")
    if not reaped:
        _failure(report, "cleanup", "CLEANUP_TIMEOUT", "The OS has not confirmed subprocess exit within the total budget.")
        report["status"] = "timeout"
        report["cleanup"]["status"] = "timeout"
    report["elapsed_seconds"] = round(time.monotonic() - started, 6)
    return report


def _main():
    if len(sys.argv) == 3 and sys.argv[1] == "--_probe":
        _probe(json.loads(sys.argv[2]))
        return 0
    parser = argparse.ArgumentParser(description="Report AOS SDK installation and bounded discovery diagnostics as JSON.")
    parser.add_argument("--config", help="Zenoh session config path; defaults to AOS_ZENOH_CONFIG, then built-in")
    parser.add_argument("--domain-id", type=int, help="Logical domain; defaults to AOS_DOMAIN_ID, then 0")
    parser.add_argument("--security-profile", help="Absolute TLS client profile; defaults to AOS_SECURITY_PROFILE")
    parser.add_argument("--timeout", type=_timeout, default=5.0, help="Total seconds including child cleanup (default: 5)")
    args = parser.parse_args()
    report = doctor_report(
        config=args.config, domain_id=args.domain_id,
        security_profile=args.security_profile, timeout=args.timeout,
    )
    print(json.dumps(report, ensure_ascii=True, allow_nan=False, indent=2))
    return 0 if report["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(_main())
