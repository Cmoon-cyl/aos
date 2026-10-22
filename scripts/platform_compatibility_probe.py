#!/usr/bin/env python3
"""Verify installed 0.1/0.2 interoperability and a local entry/config rollback.

This owns only a new output directory and its child processes. It never changes
an existing environment, system service, user configuration, or remote host.
Run with Python 3.11 -I. See docs/testing.md for scope.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.machinery
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import socket
import stat
import subprocess
import sys
import sysconfig
import time
import traceback
import uuid
import zipfile

SCRIPT = Path(__file__).resolve()
PROBE = SCRIPT.with_name("platform_probe.py")
PROTOCOL = "aos-platform-compatibility/1"


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def load(path):
    return json.loads(Path(path).read_text())


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")


def verified_build(path, router, *, baseline):
    path = Path(path).resolve(strict=True)
    proof = load(path)
    sha = proof.get("source_sha", "")
    require(re.fullmatch(r"[0-9a-f]{40}", sha), "missing build source SHA")
    require(proof.get("status") == "passed" and proof.get("tracked_changes") == "",
            "build provenance must record passing commands and a clean tracked tree")
    require(proof.get("commands") and all(step.get("returncode") == 0
                                        for step in proof["commands"]),
            "build commands are absent or failed")
    final_check = (proof.get("final_source_sha") == sha
                   and proof.get("final_source_status") == "")
    require(baseline or final_check, "candidate lacks final source/clean-tree verification")
    artifacts = {
        name: (path.parent / proof[name]["path"]).resolve(strict=True)
        for name in ("wheel", "cli")
    }
    for name, artifact in artifacts.items():
        require(digest(artifact) == proof[name]["sha256"], f"{name} differs from build proof")
    router_sha = proof["zenohd"].get("executable_sha256", proof["zenohd"].get("sha256"))
    require(digest(router) == router_sha, "router differs from build proof")
    return {
        "source_sha": sha, "proof_path": str(path), "proof_sha256": digest(path),
        "wheel": str(artifacts["wheel"]), "cli": str(artifacts["cli"]),
        "artifact_sha256": {**{k: digest(v) for k, v in artifacts.items()}, "router": router_sha},
        "historical_proof_lacks_final_source_check": not final_check,
        "source_claim": "recorded build provenance; current installed bytes checked separately",
    }


def installed_binding(build):
    import aos_sdk as aos
    from aos_sdk import _native

    require(sys.flags.isolated, "installed check requires Python -I")
    package = Path(aos.__file__).resolve()
    native = Path(_native.__file__).resolve()
    site = Path(sysconfig.get_paths()["platlib"]).resolve()
    require(package.parent == native.parent and package.parent == site / "aos_sdk"
            and isinstance(_native.__loader__, importlib.machinery.ExtensionFileLoader),
            "SDK is not this environment's installed native wheel")
    installed = {}
    with zipfile.ZipFile(build["wheel"]) as wheel:
        for member in wheel.namelist():
            if not member.startswith("aos_sdk/") or member.endswith("/"):
                continue
            relative = Path(member)
            require(not relative.is_absolute() and ".." not in relative.parts,
                    "unsafe wheel member")
            expected = hashlib.sha256(wheel.read(member)).hexdigest()
            require(digest(site / relative) == expected, f"installed file differs: {member}")
            installed[member] = expected
    require("aos_sdk/" + native.name in installed, "native module absent from wheel")
    return {
        "sdk_version": aos.__version__, "python": sys.version,
        "executable": sys.executable, "package": str(package), "native": str(native),
        "native_sha256": digest(native), "installed_files": installed, "isolated": True,
    }


def read_rows(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def validate_case(directory, provider_build, client_build, probe_hash):
    """Check actual provider executions against client results, without retries."""
    client = read_rows(directory / "client/_client-0.jsonl")
    summaries = [load(directory / f"{role}/{role}-0.json") for role in ("provider", "client")]
    require(all(row["status"] == "passed" for row in summaries), "a worker failed")
    for role, rows, build in (
        ("client", client, client_build),
        ("provider", sum((read_rows(directory / f"provider/_provider-{n}.jsonl")
                          for n in (1, 2)), []), provider_build),
    ):
        ready = [row for row in rows if row["event"] == "sdk_ready"]
        require(len(ready) == (1 if role == "client" else 2), f"{role} startup count differs")
        require(all(row["source_sha"] == build["source_sha"]
                    and row["probe_sha256"] == probe_hash
                    and row["native_sha256"] == build["installed"]["native_sha256"]
                    and row["sdk_version"] == build["installed"]["sdk_version"]
                    and row["isolated"] for row in ready), f"{role} artifact identity differs")
        require(sum(row["event"] == "sdk_closed" for row in rows) == len(ready),
                f"{role} cleanup was not observed")
    results = []
    request_ids = set()
    provider_ids = []
    for round_ in (1, 2):
        provider = read_rows(directory / f"provider/_provider-{round_}.jsonl")
        origin = next(row["origin"] for row in provider if row["event"] == "provider_ready")
        require(origin["source_sha"] == provider_build["source_sha"], "wrong provider source")
        provider_ids.append({key: origin[key] for key in ("pid", "node_id", "instance_id")})
        observed = [row for row in client if row.get("round") == round_]
        for event in ("topic_received", "service_verified", "action_success_verified",
                      "action_cancel_verified", "round_verified"):
            rows = [row for row in observed if row["event"] == event]
            require(len(rows) == 1, f"missing/duplicate client event {round_}/{event}")
            row = rows[0]
            if event == "topic_received":
                message = row["message"]
                require(message["origin"] == origin
                        and message["value"] == message["sequence"] * 3 + round_,
                        "Topic business payload differs")
            if event == "service_verified":
                executions = [p for p in provider if p["event"] == "service_executed"]
                require(len(executions) == 1 and executions[0]["request"] == row["request"]
                        and executions[0]["response"] == row["response"],
                        "Service execution/result differs or was repeated")
                require(row["response"]["sum"] == row["request"]["a"] + row["request"]["b"],
                        "Service business sum differs")
            if event in ("action_success_verified", "action_cancel_verified"):
                completed = [p for p in provider if p["event"] == "action_completed"
                             and p["goal_id"] == row["goal_id"]]
                started = [p for p in provider if p["event"] == "action_started"
                           and p["goal_id"] == row["goal_id"]]
                expected = 4 if event == "action_success_verified" else 5
                require(len(started) == len(completed) == 1
                        and started[0]["request"] == row["request"]
                        and completed[0]["result"] == row["response"]["result"]
                        and completed[0]["status"] == row["response"]["status"] == expected,
                        "Action execution/result differs or was repeated")
            if "request" in row:
                request_id = row["request"]["request_id"]
                require(request_id not in request_ids, "business request reused across cases")
                request_ids.add(request_id)
        require(sum(row["event"] == "action_started" for row in provider) == 2
                and sum(row["event"] == "action_completed" for row in provider) == 2
                and sum(row["event"] == "round_finish_requested" for row in provider) == 1,
                "unexpected provider business execution")
        results.append({"round": round_, "topic": True, "service": True,
                        "action_success": True, "action_cancel": True})
    require(all(provider_ids[0][key] != provider_ids[1][key] for key in provider_ids[0]),
            "provider restart identity did not change")
    require(sum(row["event"] == "old_provider_gone" for row in client) == 1
            and sum(row["event"] == "final_provider_gone" for row in client) == 1,
            "provider disappearance was not observed")
    return {"status": "passed", "rounds": results, "provider_instances": provider_ids,
            "unique_business_request_count": len(request_ids), "duplicate_executions": 0}


class Runner:
    def __init__(self, args):
        self.args = args
        self.out = args.output.resolve()
        self.out.mkdir(parents=True, exist_ok=False)
        self.logs = self.out / "logs"
        self.logs.mkdir()
        self.unrelated = self.out / "unrelated"
        self.unrelated.mkdir()
        self.children = []
        self.record = {
            "protocol": PROTOCOL, "status": "running", "started_unix": time.time(),
            "scope": "single-host TCP installed-version compatibility and local entry/config rollback",
            "excluded": ["native TLS mixed versions", "cross-machine", "OS service manager migration"],
            "script_sha256": digest(SCRIPT), "probe_sha256": digest(PROBE), "commands": [],
            "cases": [], "deployment_transitions": [],
        }
        self.copied = self.out / "probe"
        self.copied.mkdir()
        for source in (SCRIPT, PROBE):
            shutil.copy2(source, self.copied / source.name)
        spec = importlib.util.spec_from_file_location("compat_runtime_probe", PROBE)
        self.probe = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.probe)

    def command(self, label, command, timeout=60):
        started = time.monotonic()
        process = subprocess.run(command, cwd=self.unrelated, stdin=subprocess.DEVNULL,
                                 capture_output=True, timeout=timeout)
        (self.logs / f"{label}.stdout").write_bytes(process.stdout)
        (self.logs / f"{label}.stderr").write_bytes(process.stderr)
        self.record["commands"].append({
            "label": label, "command": list(map(str, command)), "returncode": process.returncode,
            "seconds": time.monotonic() - started,
        })
        require(process.returncode == 0, f"{label} failed; see logs")
        return process.stdout.decode()

    def bind(self, label, build):
        environment = self.out / label / "env"
        self.command(f"{label}-venv", [sys.executable, "-I", "-m", "venv", str(environment)])
        python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        self.command(f"{label}-install", [str(python), "-I", "-m", "pip", "install",
                     "--no-index", "--no-deps", build["wheel"]])
        build_path = self.out / label / "build.json"
        save(build_path, build)
        build["python"] = str(python)
        build["installed"] = json.loads(self.command(
            f"{label}-binding", [str(python), "-I", str(self.copied / SCRIPT.name),
                                "_installed", "--build", str(build_path)]))
        help_output = json.loads(self.command(f"{label}-cli-help", [build["cli"], "--help"]))
        require(help_output["ok"]
                and help_output["data"]["version"] == build["installed"]["sdk_version"],
                "CLI and SDK version labels differ")
        return build

    def spawn(self, label, command):
        with (self.logs / f"{label}.log").open("xb") as log:
            process = subprocess.Popen(command, cwd=self.unrelated, stdout=log,
                                       stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                                       start_new_session=True)
        self.children.append(process)
        return process

    def case(self, name, provider, client):
        directory = self.out / name
        directory.mkdir()
        run_id = "compat_" + uuid.uuid4().hex
        processes = []
        for role, build in (("provider", provider), ("client", client)):
            command = [
                build["python"], "-I", "-u", str(self.copied / PROBE.name), role,
                "--config", str(self.config), "--output", str(directory / role),
                "--run-id", run_id, "--source-sha", build["source_sha"],
                "--peer-source-sha", provider["source_sha"],
                "--artifact-sha256", build["artifact_sha256"]["wheel"],
                "--domain-id", "72", "--overall-timeout", "60", "--stage-timeout", "8",
                "--network-label", "single-host TCP compatibility probe",
            ]
            processes.append(self.spawn(f"{name}-{role}", command))
        deadline = time.monotonic() + 65
        try:
            while any(process.poll() is None for process in processes):
                require(time.monotonic() < deadline, f"{name} process deadline")
                require(all(process.poll() in (None, 0) for process in processes),
                        f"{name} child failed; see logs")
                time.sleep(0.05)
            require(all(p.returncode == 0 for p in processes), f"{name} child failed")
            result = validate_case(directory, provider, client, self.record["probe_sha256"])
            self.record["cases"].append({"name": name, "run_id": run_id,
                                         "provider_source": provider["source_sha"],
                                         "client_source": client["source_sha"], **result})
        finally:
            for process in processes:
                self.probe.stop_process(process, group=True)

    def switch(self, label, build):
        deployment = self.out / "deployment.json"
        temporary = self.out / "deployment.next.json"
        save(temporary, {"label": label, "python": build["python"], "cli": build["cli"],
                         "config": str(self.config), "source_sha": build["source_sha"]})
        os.replace(temporary, deployment)
        selected = load(deployment)
        require(selected["python"] == build["python"]
                and selected["source_sha"] == build["source_sha"], "entry switch differs")
        self.record["deployment_transitions"].append({
            **selected, "deployment_sha256": digest(deployment), "unix": time.time(),
        })
        # The selected entry, rather than a hard-coded interpreter, launches
        # both real processes in upgrade/rollback cases.
        return {**build, "python": selected["python"], "cli": selected["cli"]}

    def run(self):
        try:
            require(sys.version_info[:2] == (3, 11) and sys.flags.isolated,
                    "use CPython 3.11 with -I")
            builds = {
                "baseline": verified_build(self.args.baseline_provenance, self.args.router, baseline=True),
                "candidate": verified_build(self.args.candidate_provenance, self.args.router, baseline=False),
            }
            builds = {label: self.bind(label, build) for label, build in builds.items()}
            require(builds["baseline"]["installed"]["sdk_version"] == "0.1.0"
                    and builds["candidate"]["installed"]["sdk_version"] == "0.2.0",
                    "expected baseline 0.1.0 and candidate 0.2.0")
            self.record["builds"] = builds
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                port = listener.getsockname()[1]
            endpoint = f"tcp/127.0.0.1:{port}"
            router_config = self.out / "router.json"
            save(router_config, {
                "mode": "router", "listen": {"endpoints": [endpoint]},
                "scouting": {"multicast": {"enabled": False},
                             "gossip": {"enabled": False, "autoconnect": []}},
            })
            self.config = self.out / "session.json"
            save(self.config, {
                "mode": "client", "connect": {"endpoints": [endpoint], "timeout_ms": 3000,
                                             "exit_on_failure": True},
                "listen": {"endpoints": []},
                "scouting": {"multicast": {"enabled": False},
                             "gossip": {"enabled": False, "autoconnect": []}},
                "timestamping": {"enabled": True},
            })
            self.config.chmod(0o600)
            backup = self.out / "backup"
            backup.mkdir()
            shutil.copy2(self.config, backup / self.config.name)
            initial = {"sha256": digest(self.config), "mode": stat.S_IMODE(self.config.stat().st_mode)}
            router = self.spawn("router", [str(self.args.router), "-c", str(router_config)])
            deadline = time.monotonic() + 8
            while True:
                require(router.poll() is None, "router failed to start")
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                        break
                except OSError:
                    require(time.monotonic() < deadline, "router listen deadline")
                    time.sleep(0.02)
            baseline, candidate = builds["baseline"], builds["candidate"]
            old = self.switch("baseline-before-upgrade", baseline)
            shutil.copy2(self.out / "deployment.json", backup / "deployment.json")
            self.case("baseline-before-upgrade", old, old)
            self.case("old-provider-new-client", baseline, candidate)
            self.case("new-provider-old-client", candidate, baseline)
            new = self.switch("candidate-upgrade", candidate)
            self.case("candidate-upgrade", new, new)
            # Restore the actual saved configuration and entry, then exercise
            # the restored interpreter. Old wheel/environment stays intact.
            shutil.copy2(backup / self.config.name, self.config)
            shutil.copy2(backup / "deployment.json", self.out / "deployment.restore.json")
            os.replace(self.out / "deployment.restore.json", self.out / "deployment.json")
            restored = load(self.out / "deployment.json")
            require(restored["source_sha"] == baseline["source_sha"], "rollback entry differs")
            self.record["deployment_transitions"].append({
                **restored, "operation": "restore-backup", "unix": time.time(),
                "deployment_sha256": digest(self.out / "deployment.json"),
            })
            self.case("baseline-after-rollback", {**baseline, "python": restored["python"]},
                      {**baseline, "python": restored["python"]})
            final = {"sha256": digest(self.config), "mode": stat.S_IMODE(self.config.stat().st_mode)}
            require(final == initial, "rollback changed configuration bytes or permissions")
            self.record["configuration"] = {"before": initial, "after": final,
                                            "backup_sha256": digest(backup / self.config.name)}
            for label, build in builds.items():
                after = json.loads(self.command(
                    f"{label}-binding-after", [build["python"], "-I",
                        str(self.copied / SCRIPT.name), "_installed",
                        "--build", str(self.out / label / "build.json")]))
                require(after == build["installed"], f"{label} installed files changed")
                verified_build(build["proof_path"], self.args.router, baseline=label == "baseline")
            require(digest(SCRIPT) == self.record["script_sha256"]
                    and digest(PROBE) == self.record["probe_sha256"], "harness changed during run")
            self.record["status"] = "passed"
        except BaseException as error:
            self.record["status"] = "failed"
            self.record["error"] = f"{type(error).__name__}: {error}"
            traceback.print_exc()
        finally:
            for process in self.children:
                self.probe.stop_process(process, group=True)
            self.record["children"] = [{"pid": p.pid, "returncode": p.returncode}
                                       for p in self.children]
            self.record["all_children_reaped"] = all(p.poll() is not None for p in self.children)
            self.record["finished_unix"] = time.time()
            save(self.out / "result.json", self.record)
            print(json.dumps({"status": self.record["status"], "result": str(self.out / "result.json"),
                              "cases_passed": len(self.record["cases"]),
                              "error": self.record.get("error")}), flush=True)
        return 0 if self.record["status"] == "passed" else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="mode", required=True)
    run = subparsers.add_parser("run")
    for name in ("baseline-provenance", "candidate-provenance", "router", "output"):
        run.add_argument("--" + name, type=Path, required=True)
    installed = subparsers.add_parser("_installed")
    installed.add_argument("--build", type=Path, required=True)
    args = parser.parse_args()
    if args.mode == "_installed":
        print(json.dumps(installed_binding(load(args.build))))
        return 0
    return Runner(args).run()


if __name__ == "__main__":
    raise SystemExit(main())
