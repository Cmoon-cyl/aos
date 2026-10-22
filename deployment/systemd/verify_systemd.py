#!/usr/bin/env python3
"""Run scoped real-systemd fixtures on Linux; never claim SDK acceptance.

Every test owns a fresh aos-<name>-*.service. State changes are restricted to
these units. Interrupted runs can be cleaned up with the same --name/root.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/system_service.py"


def run(argv, check=True):
    result = subprocess.run(argv, text=True, capture_output=True, timeout=60)
    if check and result.returncode:
        raise AssertionError({"argv": argv, "code": result.returncode,
                              "stdout": result.stdout, "stderr": result.stderr})
    return result


def service(action, name=None, *extra, check=True):
    argv = [sys.executable, str(SCRIPT), action]
    if name:
        argv += ["--name", name]
    result = run(argv + list(extra), check=check)
    data = json.loads(result.stdout)
    if check and not data["ok"]:
        raise AssertionError(data)
    return data["data"] if data["ok"] else data


def wait(predicate, seconds=15):
    deadline = time.monotonic() + seconds
    latest = None
    while time.monotonic() < deadline:
        latest = predicate()
        if latest:
            return latest
        time.sleep(0.1)
    raise AssertionError({"wait_timeout": seconds, "latest": latest})


def records(path):
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def started(path):
    return [value for value in records(path) if value["event"] == "started"]


def alive(pid):
    try:
        return Path(f"/proc/{pid}/stat").read_text().rpartition(")")[2].split()[0] != "Z"
    except FileNotFoundError:
        return False


def request(root, suffix, mode):
    path = root / f"{suffix}.json"
    output = root / f"{suffix}.jsonl"
    value = {
        "prefixes": [str(root)],
        "plan": {"schema_version": 1,
                 "processes": [{"id": "fixture", "package": "fixture", "entry": "fixture",
                                "env": {"FIXTURE_MODE": mode, "FIXTURE_RECORD": str(output)}}],
                 "shutdown": {"sigint_timeout": 0.2, "sigterm_timeout": 0.2}},
    }
    path.write_text(json.dumps(value))
    return path, output


def fixture(root):
    directory = root / 'binary $AOS_TEST% space'
    directory.mkdir(exist_ok=True)
    path = directory / 'supervisor $AOS_TEST%'
    if not path.exists():
        shutil.copyfile(ROOT / "deployment/systemd/fixture_supervisor.py", path)
        path.chmod(0o700)
    return path


def install(root, name, suffix, mode, *extra):
    binary = fixture(root)
    request_directory = root / 'literal "$AOS_TEST"% space'
    request_directory.mkdir(exist_ok=True)
    source, output = request(request_directory, suffix, mode)
    render = service("render", name, "--aos", str(binary), "--request", str(source),
                     "--restart-delay", "5", "--start-limit-interval", "60",
                     "--start-limit-burst", "2", *extra,
                     "--output", str(root / f"aos-{name}.service"))
    verify = run(["systemd-analyze", "--user", "verify", render["unit_path"]])
    assert not verify.stderr, verify.stderr
    service("install", name, "--aos", str(binary), "--request", str(source),
            "--restart-delay", "5", "--start-limit-interval", "60", "--start-limit-burst", "2",
            "--enable", "--start", *extra)
    return output


def cleanup(names):
    result = []
    for name in names:
        result.append(service("uninstall", name, check=False))
    return result


def fixtures(args):
    root = Path(args.root).resolve()
    root.mkdir(parents=True, exist_ok=False)
    evidence = {"kind": "systemd_fixture_not_sdk", "audit": service("audit"), "tests": {}}
    names = [args.name + "-crash", args.name + "-timeout", args.name + "-failure"]
    try:
        output = install(root, names[0], "crash", "normal")
        first = wait(lambda: started(output) and started(output)[0])
        state = service("status", names[0])
        assert int(state["systemd"]["MainPID"]) == first["pid"]
        assert first["request"] == str(output.with_suffix(".json"))
        assert first["cwd"] == str(output.parent)
        assert first["child"] in state["cgroup_pids"]
        # Name collision is rejected before any existing unit is touched.
        collision = service("install", names[0], "--aos", str(fixture(root)),
                            "--request", first["request"], check=False)
        assert collision["error"]["code"] == "ALREADY_EXISTS", collision
        before_kill = time.monotonic()
        run(["systemctl", "--user", "kill", "--kill-whom=main", "--signal=KILL",
             f"aos-{names[0]}.service"])
        second = wait(lambda: len(started(output)) >= 2 and started(output)[1])
        assert second["pid"] != first["pid"]
        assert not alive(first["pid"]) and not alive(first["child"])
        elapsed = time.monotonic() - before_kill
        assert elapsed >= 4.8, elapsed
        run(["systemctl", "--user", "kill", "--kill-whom=main", "--signal=KILL",
             f"aos-{names[0]}.service"])
        # systemd 252 can retain Result=signal after the subsequent start-limit
        # rejection. Use observed failed state, restart count and the journal.
        failed = wait(lambda: (s := service("status", names[0]))["systemd"]["ActiveState"]
                      == "failed" and s["systemd"]["NRestarts"] == "2" and s)
        assert not alive(second["pid"]) and not alive(second["child"])
        time.sleep(0.5)
        assert len(started(output)) == 2
        limit_logs = service("logs", names[0], "--lines", "50")
        assert any("Start request repeated too quickly" in item.get("MESSAGE", "")
                   for item in limit_logs["records"]), limit_logs
        evidence["tests"]["supervisor_crash_and_start_limit"] = {
            "starts": started(output), "restart_delay_observed_seconds": elapsed,
            "final_status": failed, "collision": collision, "journal": limit_logs["records"],
        }
        output = install(root, names[1], "timeout", "timeout",
                         "--stop-timeout", "1", "--allow-forced-stop")
        current = wait(lambda: started(output) and started(output)[0])
        before = time.monotonic()
        stopped = service("stop", names[1])
        elapsed = time.monotonic() - before
        assert not alive(current["pid"]) and not alive(current["child"])
        assert 0.8 <= elapsed < 5, elapsed
        evidence["tests"]["forced_timeout_cgroup_cleanup"] = {
            "start": current, "stop_seconds": elapsed, "status": stopped,
        }
        output = install(root, names[2], "failure", "failure")
        wait(lambda: records(output))
        time.sleep(5.5)
        failed = service("status", names[2])
        assert len(records(output)) == 1
        assert failed["systemd"]["NRestarts"] == "0", failed
        assert failed["systemd"]["ExecMainStatus"] == "1", failed
        evidence["tests"]["reported_failure_not_restarted"] = {
            "records": records(output), "status": failed,
        }
        logs = service("logs", names[0], "--lines", "50")
        assert logs["records"], "fixture output absent from journal"
        evidence["journal_record_count"] = len(logs["records"])
        evidence["passed"] = True
    finally:
        evidence["cleanup"] = cleanup(names)
        (root / "evidence.json").write_text(json.dumps(evidence, indent=2))
    assert all(item.get("removed") and item.get("load_state") == "not-found"
               and not item["remaining_pids"] for item in evidence["cleanup"]), evidence["cleanup"]
    return evidence


def persistence_start(args):
    root = Path(args.root).resolve()
    root.mkdir(parents=True, exist_ok=False)
    output = install(root, args.name, "session", "normal")
    first = wait(lambda: started(output) and started(output)[0])
    value = {"start": first, "status": service("status", args.name), "audit": service("audit")}
    (root / "before-ssh-exit.json").write_text(json.dumps(value, indent=2))
    return value


def persistence_check(args):
    root = Path(args.root).resolve()
    before = json.loads((root / "before-ssh-exit.json").read_text())
    value = {"before": before, "after": service("status", args.name), "audit_after": service("audit"),
             "scope": "previous SSH session ended; other user sessions were not terminated"}
    try:
        assert before["status"]["systemd"]["MainPID"] == value["after"]["systemd"]["MainPID"]
        assert value["after"]["systemd"]["ActiveState"] == "active"
        value["graceful_stop"] = service("stop", args.name)
        assert value["graceful_stop"]["systemd"]["ExecMainStatus"] == "143"
        assert value["graceful_stop"]["systemd"]["Result"] == "success"
        assert not alive(before["start"]["pid"]) and not alive(before["start"]["child"])
        value["passed"] = True
    finally:
        value["cleanup"] = service("uninstall", args.name)
        (root / "after-ssh-exit.json").write_text(json.dumps(value, indent=2))
    return value


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["fixtures", "persistence-start", "persistence-check"])
    parser.add_argument("--root", required=True)
    parser.add_argument("--name", required=True)
    arguments = parser.parse_args()
    function = {"fixtures": fixtures, "persistence-start": persistence_start,
                "persistence-check": persistence_check}[arguments.action]
    result = function(arguments)
    print(json.dumps({"action": arguments.action, "root": str(Path(arguments.root).resolve()),
                      "passed": result.get("passed"), "tests": list(result.get("tests", {})),
                      "main_pid": result.get("status", {}).get("systemd", {}).get("MainPID"),
                      "cleanup": result.get("cleanup")}, indent=2))
