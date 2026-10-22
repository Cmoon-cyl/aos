#!/usr/bin/env python3
"""Install and inspect an explicitly owned AOS systemd *user* service.

Uses only the Python standard library. The manager owns the supervisor cgroup;
the supervisor alone owns child restart policy. No application inputs or
environment secrets are copied into the unit or ownership record.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile
from typing import Any

SCHEMA = 1
MAX_JSON = 1024 * 1024
MARKER = "# Managed by aos scripts/system_service.py; schema=1\n"
STDOUT_STALL_SECONDS = 30
SHUTDOWN_OUTPUT_SECONDS = 30
PROBE_CLOSE_SECONDS = 1
TERMINAL_OUTPUT_SECONDS = 2
SHOW = (
    "Id", "LoadState", "ActiveState", "SubState", "MainPID", "ControlGroup",
    "NRestarts", "Result", "ExecMainStatus", "ExecMainCode", "UnitFileState",
    "FragmentPath", "DropInPaths", "Restart", "RestartUSec", "KillMode",
    "KillSignal", "FinalKillSignal", "SendSIGKILL", "TimeoutStopUSec",
    "StartLimitIntervalUSec", "StartLimitBurst",
)


class ServiceError(Exception):
    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def fail(code: str, message: str) -> Any:
    raise ServiceError(code, message)


def unit_name(name: str) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,47}", name):
        fail("INVALID_NAME", "name must match [a-z][a-z0-9-]{0,47}")
    return f"aos-{name}.service"


def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in items:
        if key in result:
            fail("INVALID_JSON", "duplicate object key")
        result[key] = value
    return result


def strict_json(raw: bytes) -> dict[str, Any]:
    if len(raw) > MAX_JSON:
        fail("INVALID_JSON", "JSON input exceeds 1 MiB")
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs,
                           parse_constant=lambda _: fail("INVALID_JSON", "non-finite number"))
    except (ValueError, UnicodeError):
        fail("INVALID_JSON", "input must be valid UTF-8 JSON")
    if not isinstance(value, dict):
        fail("INVALID_JSON", "top-level JSON must be an object")
    return value


def read_json(path: Path) -> dict[str, Any]:
    with path.open("rb") as stream:
        return strict_json(stream.read(MAX_JSON + 1))


def absolute(value: str, kind: str, executable: bool = False) -> Path:
    if not isinstance(value, str) or any(ord(c) < 32 or ord(c) == 127 for c in value):
        fail("INVALID_PATH", "paths cannot contain control characters")
    path = Path(value)
    if not path.is_absolute():
        fail("INVALID_PATH", "service paths must be absolute")
    try:
        path = path.resolve(strict=True)
    except OSError:
        fail("INVALID_PATH", "service path does not exist")
    if (kind == "file" and not path.is_file()) or (kind == "directory" and not path.is_dir()):
        fail("INVALID_PATH", f"service path must be a {kind}")
    if executable and not os.access(path, os.X_OK):
        fail("INVALID_PATH", "aos must be executable")
    return path


def seconds(value: Any, name: str, minimum: float = 0.0) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        fail("INVALID_ARGUMENT", f"{name} must be a finite number")
    if not minimum <= value <= 86400 or not math.isfinite(value):
        fail("INVALID_ARGUMENT", f"{name} must be between {minimum:g} and 86400 seconds")
    return float(value)


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(65536), b""):
            value.update(block)
    return value.hexdigest()


def quote(value: str) -> str:
    """systemd word quoting, including literal % specifiers; never a shell."""
    if any(ord(c) < 32 or ord(c) == 127 for c in value):
        fail("INVALID_PATH", "unit values cannot contain control characters")
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"').replace("%", "%%") + '"'


def directory_value(value: str) -> str:
    # WorkingDirectory is a scalar path, not a systemd command-line word.
    # Quotes are literal here. Continuation and trailing-space ambiguity are
    # rejected instead of silently changing the directory.
    if any(ord(c) < 32 or ord(c) == 127 for c in value) or value.endswith(("\\", " ")):
        fail("INVALID_PATH", "working directory cannot end in space/backslash or contain controls")
    return value.replace("%", "%%")


def command(args: list[str], *, timeout: float = 15, data: str | None = None,
            check: bool = True) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(args, input=data, text=True, capture_output=True,
                                timeout=timeout, check=False)
    except FileNotFoundError:
        fail("COMMAND_UNAVAILABLE", f"required program unavailable: {Path(args[0]).name}")
    except subprocess.TimeoutExpired:
        fail("COMMAND_TIMEOUT", f"{Path(args[0]).name} exceeded its local command deadline")
    if check and result.returncode:
        # Do not echo arbitrary subprocess diagnostics or user configuration.
        fail("COMMAND_FAILED", f"{Path(args[0]).name} exited with status {result.returncode}")
    return result


def properties(text: str) -> dict[str, str]:
    return dict(line.split("=", 1) for line in text.splitlines() if "=" in line)


def systemctl(*args: str, timeout: float = 15, check: bool = True) -> subprocess.CompletedProcess[str]:
    return command(["systemctl", "--user", *args], timeout=timeout, check=check)


def show(unit: str) -> dict[str, str]:
    result = systemctl("show", unit, "--no-pager", "--property=" + ",".join(SHOW), check=False)
    value = properties(result.stdout)
    if not value.get("LoadState"):
        fail("USER_MANAGER_UNAVAILABLE", "cannot query the current systemd user manager")
    return value


def input_plan(aos: Path, request_path: Path) -> tuple[dict[str, Any], dict[str, str]]:
    request = read_json(request_path)
    allowed = {"prefixes", "plan", "path", "package", "resource", "package_version", "overrides"}
    if set(request) - allowed:
        fail("INVALID_REQUEST", "unsupported launch request field")
    prefixes = request.get("prefixes")
    if not isinstance(prefixes, list) or not prefixes:
        fail("INVALID_REQUEST", "system services require a nonempty explicit prefixes array")
    for prefix in prefixes:
        absolute(prefix, "directory")
    modes = sum(key in request for key in ("plan", "path", "package"))
    if modes != 1:
        fail("INVALID_REQUEST", "choose exactly one launch source: plan, path, or package/resource")
    tracked = {str(request_path): digest(request_path)}
    if "plan" in request:
        if "resource" in request or "package_version" in request:
            fail("INVALID_REQUEST", "resource/version require the package source")
        plan = request["plan"]
    elif "path" in request:
        if "resource" in request or "package_version" in request:
            fail("INVALID_REQUEST", "resource/version require the package source")
        source = absolute(request["path"], "file")
        plan = read_json(source)
        tracked[str(source)] = digest(source)
    else:
        if not isinstance(request.get("resource"), str) or not request["resource"]:
            fail("INVALID_REQUEST", "package source requires a resource")
        lookup = {key: request[key] for key in
                  ("prefixes", "package", "resource", "package_version") if key in request}
        result = command([str(aos), "package", "resource", "--request", "-"],
                         data=json.dumps(lookup), timeout=15)
        response = strict_json(result.stdout.encode())
        resource = response.get("data", {})
        if not isinstance(resource, dict) or resource.get("kind") != "launch":
            fail("INVALID_REQUEST", "package resource must resolve to a launch resource")
        source = absolute(resource.get("path"), "file")
        plan = read_json(source)
        tracked[str(source)] = digest(source)
    if not isinstance(plan, dict) or type(plan.get("schema_version")) is not int or plan["schema_version"] != 1:
        fail("INVALID_REQUEST", "launch plan requires schema_version 1")
    if not isinstance(plan.get("processes"), list) or not plan["processes"]:
        fail("INVALID_REQUEST", "launch plan requires processes")
    return plan, tracked


def render(args: argparse.Namespace) -> tuple[str, dict[str, Any]]:
    name = unit_name(args.name)
    aos = absolute(args.aos, "file", executable=True)
    if any(char in str(aos) for char in "\"'\\"):
        fail("INVALID_PATH", "systemd executable paths cannot contain quotes or backslashes")
    request = absolute(args.request, "file")
    working = absolute(args.working_directory, "directory") if args.working_directory else request.parent
    plan, tracked = input_plan(aos, request)
    shutdown = plan.get("shutdown", {})
    if not isinstance(shutdown, dict) or set(shutdown) - {"sigint_timeout", "sigterm_timeout"}:
        fail("INVALID_REQUEST", "invalid shutdown object")
    sigint = seconds(shutdown.get("sigint_timeout", 5), "sigint_timeout")
    sigterm = seconds(shutdown.get("sigterm_timeout", 5), "sigterm_timeout")
    # Conservatively add the independent limits. Output drain and group cleanup
    # may overlap, but a blocked journal must not make systemd kill the CLI
    # before its own bounded cleanup and final-output deadlines have elapsed.
    graceful = (sigint + sigterm + 2 + SHUTDOWN_OUTPUT_SECONDS
                + PROBE_CLOSE_SECONDS + TERMINAL_OUTPUT_SECONDS + 3)
    stop_timeout = graceful if args.stop_timeout is None else seconds(args.stop_timeout, "stop_timeout", 0.1)
    if stop_timeout < graceful and not args.allow_forced_stop:
        fail("SHORT_STOP_TIMEOUT", "shorter OS stop timeout requires --allow-forced-stop")
    delay = seconds(args.restart_delay, "restart_delay", 5)
    interval = seconds(args.start_limit_interval, "start_limit_interval", 10)
    burst = args.start_limit_burst
    if type(burst) is not int or not 1 <= burst <= 5:
        fail("INVALID_ARGUMENT", "start_limit_burst must be an integer between 1 and 5")
    if interval <= delay * burst:
        fail("INVALID_ARGUMENT", "start-limit interval must exceed delay times burst")
    content = MARKER + f"""[Unit]
Description=AOS launch supervisor ({args.name})
StartLimitIntervalSec={interval:g}s
StartLimitBurst={burst}
StartLimitAction=none

[Service]
Type=exec
WorkingDirectory={directory_value(str(working))}
ExecStart=:{quote(str(aos))} launch start --request {quote(str(request))}
Restart=on-failure
RestartPreventExitStatus=1 2 3 4 129 130 131 143
SuccessExitStatus=129 130 131 143
RestartSec={delay:g}s
KillMode=mixed
KillSignal=SIGTERM
FinalKillSignal=SIGKILL
SendSIGKILL=yes
TimeoutStopSec={stop_timeout:g}s
StandardInput=null
StandardOutput=journal
StandardError=journal
SyslogIdentifier={name.removesuffix(".service")}
UMask=0077

[Install]
WantedBy=default.target
"""
    record = {
        "schema_version": SCHEMA, "unit": name, "uid": os.getuid(),
        "unit_sha256": hashlib.sha256(content.encode()).hexdigest(),
        "aos": str(aos), "request": str(request), "working_directory": str(working),
        "tracked_inputs": tracked, "stop_timeout_seconds": stop_timeout,
        "graceful_budget_seconds": graceful, "forced_stop": stop_timeout < graceful,
        "stdout_stall_seconds": STDOUT_STALL_SECONDS,
        "shutdown_output_seconds": SHUTDOWN_OUTPUT_SECONDS,
        "probe_close_seconds": PROBE_CLOSE_SECONDS,
        "terminal_output_seconds": TERMINAL_OUTPUT_SECONDS,
        "restart_delay_seconds": delay, "start_limit_interval_seconds": interval,
        "start_limit_burst": burst,
    }
    return content, record


def roots() -> tuple[Path, Path]:
    home = Path.home()
    config = Path(os.environ.get("XDG_CONFIG_HOME", home / ".config"))
    state = Path(os.environ.get("XDG_STATE_HOME", home / ".local/state"))
    if not config.is_absolute() or not state.is_absolute():
        fail("INVALID_PATH", "XDG configuration and state directories must be absolute")
    return config / "systemd/user", state / "aos/system-services"


def locations(name: str) -> tuple[Path, Path]:
    unit = unit_name(name)
    config, state = roots()
    return config / unit, state / unit / "owner.json"


def exclusive_write(path: Path, data: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def owned_file(path: Path) -> None:
    try:
        info = path.lstat()
    except FileNotFoundError:
        fail("NOT_OWNED", "no matching installed ownership record and unit")
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o022:
        fail("NOT_OWNED", "owned files must be regular, current-user files without external write access")


def owned(name: str, *, manager: bool = True) -> tuple[Path, Path, dict[str, Any], dict[str, str]]:
    path, owner = locations(name)
    owned_file(path)
    owned_file(owner)
    record = read_json(owner)
    if record.get("schema_version") != SCHEMA or record.get("uid") != os.getuid() or record.get("unit") != path.name:
        fail("NOT_OWNED", "ownership metadata does not match this service")
    if not path.read_bytes().startswith(MARKER.encode()) or digest(path) != record.get("unit_sha256"):
        fail("UNIT_MODIFIED", "unit changed after installation; preserve and review it before changing service state")
    current = show(path.name) if manager else {}
    if manager and (current.get("FragmentPath") != str(path) or current.get("DropInPaths")):
        fail("UNIT_MODIFIED", "loaded service differs from the owned unit or has additional drop-ins")
    return path, owner, record, current


def changed_inputs(record: dict[str, Any]) -> list[str]:
    changed = []
    for path, expected in record["tracked_inputs"].items():
        try:
            actual = digest(Path(path))
        except OSError:
            actual = None
        if actual != expected:
            changed.append(path)
    return changed


def cgroup_mount(control_group: str) -> Path | None:
    if not control_group or not control_group.startswith("/") or ".." in control_group.split("/"):
        return None
    try:
        lines = Path("/proc/self/mountinfo").read_text().splitlines()
    except OSError:
        return None
    for line in lines:
        fields = line.split()
        split = fields.index("-")
        fs_type = fields[split + 1]
        options = fields[split + 3].split(",")
        if fs_type != "cgroup2" and not (fs_type == "cgroup" and "name=systemd" in options):
            continue
        decode = lambda s: re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), s)
        mount_root, mount = decode(fields[3]), Path(decode(fields[4]))
        try:
            relative = Path(control_group).relative_to(mount_root)
        except ValueError:
            continue
        return mount / relative
    return None


def cgroup_pids(control_group: str) -> list[int] | None:
    if not control_group:
        return []
    root = cgroup_mount(control_group)
    if root is None:
        return None
    if not root.exists():
        return []
    pids = set()
    count = 0
    for directory, subdirs, _ in os.walk(root, followlinks=False):
        subdirs[:] = [name for name in subdirs if not (Path(directory) / name).is_symlink()]
        count += 1
        if count > 1024:
            fail("CGROUP_TOO_LARGE", "owned cgroup exceeds the inspection bound")
        try:
            pids.update(int(pid) for pid in (Path(directory) / "cgroup.procs").read_text().split())
        except FileNotFoundError:
            continue
        except (PermissionError, ValueError):
            return None
    return sorted(pids)


def status(name: str) -> dict[str, Any]:
    path, _, record, current = owned(name)
    return {
        "unit": path.name, "systemd": current,
        "cgroup_pids": cgroup_pids(current.get("ControlGroup", "")),
        "changed_inputs": changed_inputs(record),
        "stop_timeout_seconds": record["stop_timeout_seconds"],
        "graceful_budget_seconds": record["graceful_budget_seconds"],
        "forced_stop": record["forced_stop"],
    }


def stop(name: str) -> dict[str, Any]:
    path, _, record, before = owned(name)
    systemctl("stop", path.name, timeout=record["stop_timeout_seconds"] + 20)
    after = show(path.name)
    old_group = before.get("ControlGroup", "")
    remaining = cgroup_pids(old_group)
    if remaining is None:
        fail("CLEANUP_UNVERIFIED", "systemd stop returned but cgroup inspection is unavailable")
    if remaining or after.get("MainPID") not in ("0", ""):
        fail("CLEANUP_INCOMPLETE", "owned cgroup still contains processes after systemd stop")
    return {"unit": path.name, "systemd": after, "previous_control_group": old_group,
            "remaining_pids": remaining}


def install(args: argparse.Namespace) -> dict[str, Any]:
    content, record = render(args)
    path, owner = locations(args.name)
    # An existing loaded unit, path, symlink, or record is never silently replaced.
    if path.exists() or path.is_symlink() or owner.exists() or owner.is_symlink():
        fail("ALREADY_EXISTS", "unit or ownership record exists; inspect and uninstall the owned unit first")
    current = show(path.name)
    if current["LoadState"] != "not-found":
        fail("ALREADY_EXISTS", "a service with this name is already known to systemd")
    with tempfile.TemporaryDirectory(prefix="aos-unit-verify-") as directory:
        candidate = Path(directory) / path.name
        candidate.write_text(content)
        checked = command(["systemd-analyze", "--user", "verify", str(candidate)], check=False)
        if checked.returncode:
            fail("UNIT_INVALID", "systemd-analyze rejected the generated service before installation")
    path.parent.mkdir(parents=True, exist_ok=True)
    owner.parent.mkdir(mode=0o700, parents=True, exist_ok=False)
    wrote_unit = False
    try:
        exclusive_write(path, content.encode())
        wrote_unit = True
        exclusive_write(owner, (json.dumps(record, indent=2) + "\n").encode())
    except BaseException:
        if wrote_unit and path.exists() and digest(path) == record["unit_sha256"]:
            path.unlink()
        if not owner.exists():
            owner.parent.rmdir()
        raise
    # On manager failure retain the owned files for inspection and explicit cleanup.
    systemctl("daemon-reload")
    owned(args.name)
    if args.enable:
        systemctl("enable", path.name)
    if args.start:
        systemctl("start", path.name)
    return status(args.name)


def uninstall(name: str) -> dict[str, Any]:
    path, owner, _, _ = owned(name)
    stopped = stop(name)
    systemctl("disable", path.name)
    # Recheck before deleting: another process may have edited the unit during stop.
    owned(name)
    path.unlink()
    owner.unlink()
    owner.parent.rmdir()
    systemctl("daemon-reload")
    systemctl("reset-failed", path.name, check=False)
    return {"unit": path.name, "removed": True, "remaining_pids": stopped["remaining_pids"],
            "load_state": show(path.name).get("LoadState")}


def audit() -> dict[str, Any]:
    if sys.platform != "linux":
        fail("UNSUPPORTED", "systemd user service management requires Linux")
    manager = systemctl("is-system-running", check=False)
    login = command(["loginctl", "show-user", str(os.getuid()), "-p", "Linger", "-p", "State",
                     "-p", "Sessions", "-p", "RuntimePath"],
                    check=False)
    version = command(["systemctl", "--version"]).stdout.splitlines()[0]
    return {
        "platform": sys.platform, "uid": os.getuid(), "systemd_version": version,
        "user_manager": manager.stdout.strip(), "login": properties(login.stdout),
        "login_query_exit": login.returncode,
        "cgroup_version": 2 if Path("/sys/fs/cgroup/cgroup.controllers").exists() else 1,
        "linger_modified": False, "requires_sudo": False,
    }


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(dest="action", required=True)
    commands.add_parser("audit", help="read manager, cgroup and login/linger conditions")
    for action in ("render", "install"):
        item = commands.add_parser(action)
        item.add_argument("--name", required=True)
        item.add_argument("--aos", required=True)
        item.add_argument("--request", required=True)
        item.add_argument("--working-directory")
        item.add_argument("--restart-delay", type=float, default=10)
        item.add_argument("--start-limit-interval", type=float, default=300)
        item.add_argument("--start-limit-burst", type=int, default=3)
        item.add_argument("--stop-timeout", type=float)
        item.add_argument("--allow-forced-stop", action="store_true")
        if action == "render":
            item.add_argument("--output", help="create a new unit file; never overwrite")
        else:
            item.add_argument("--enable", action="store_true", help="start at the user manager's default target")
            item.add_argument("--start", action="store_true")
    for action in ("start", "stop", "status", "logs", "uninstall", "reset-failed"):
        item = commands.add_parser(action)
        item.add_argument("--name", required=True)
        if action == "logs":
            item.add_argument("--lines", type=int, default=100)
    return result


def execute(args: argparse.Namespace) -> dict[str, Any]:
    if args.action == "render":
        content, record = render(args)
        if args.output:
            output = Path(args.output)
            if not output.is_absolute():
                fail("INVALID_PATH", "output must be absolute")
            exclusive_write(output, content.encode())
            return {"unit_path": str(output), "configuration": record}
        return {"unit_text": content, "configuration": record}
    if sys.platform != "linux":
        fail("UNSUPPORTED", "systemd user service management requires Linux")
    if args.action == "audit":
        return audit()
    if args.action == "install":
        return install(args)
    if args.action == "status":
        return status(args.name)
    if args.action == "stop":
        return stop(args.name)
    if args.action == "uninstall":
        return uninstall(args.name)
    path, _, record, _ = owned(args.name)
    if args.action == "start":
        if changed_inputs(record):
            fail("INPUT_CHANGED", "launch input changed; reinstall to recalculate the stop budget")
        systemctl("start", path.name)
        return status(args.name)
    if args.action == "reset-failed":
        systemctl("reset-failed", path.name)
        return status(args.name)
    if not 1 <= args.lines <= 1000:
        fail("INVALID_ARGUMENT", "log lines must be between 1 and 1000")
    logs = command(["journalctl", "--user-unit", path.name, "--no-pager", "-n", str(args.lines),
                    "--output=json"])
    fields = {"__REALTIME_TIMESTAMP", "__MONOTONIC_TIMESTAMP", "_PID", "MESSAGE", "PRIORITY",
              "_SYSTEMD_USER_UNIT", "USER_UNIT", "MESSAGE_ID", "N_RESTARTS", "EXIT_CODE",
              "EXIT_STATUS", "UNIT_RESULT"}
    records = [{key: value for key, value in json.loads(line).items() if key in fields}
               for line in logs.stdout.splitlines() if line]
    return {"unit": path.name, "records": records}


def main() -> int:
    args = parser().parse_args()
    try:
        data = execute(args)
        print(json.dumps({"schema_version": SCHEMA, "ok": True, "data": data}, ensure_ascii=False))
        return 0
    except ServiceError as error:
        print(json.dumps({"schema_version": SCHEMA, "ok": False,
                          "error": {"code": error.code, "message": str(error)}}))
        return 1
    except OSError as error:
        print(json.dumps({"schema_version": SCHEMA, "ok": False,
                          "error": {"code": "FILE_IO_ERROR", "message": error.strerror}}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
