#!/usr/bin/env python3
"""Exercise installed AOS programs, bounded restart, and owned-group cleanup.

Run with an installed-wheel Python and -I, from outside the checkout. This
script owns only a new output directory, its child processes, and (in systemd
modes) the one user unit named in that directory's owner record. It neither
builds artifacts nor changes production sources, existing prefixes, or linger.
"""
from __future__ import annotations

import argparse
from collections import deque
from copy import deepcopy
import hashlib
import importlib.machinery
import json
import os
from pathlib import Path
import platform
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import sysconfig
import threading
import time
import traceback
import uuid
import zipfile

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = Path(__file__).resolve()
PROTOCOL = "aos-stage4-program-probe/1"


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n",
                    encoding="utf-8")


def clean_environment():
    return {key: value for key, value in os.environ.items()
            if not key.startswith("AOS_") and key not in ("PYTHONPATH", "PYTHONHOME")}


def process_table():
    """Read only numeric identities; never persist unrelated process arguments."""
    result = subprocess.run(["ps", "-axo", "pid=,ppid=,pgid=,stat="],
                            text=True, capture_output=True, timeout=5, check=True)
    records = {}
    for line in result.stdout.splitlines():
        pid, ppid, pgid, state = line.split()
        records[int(pid)] = dict(pid=int(pid), ppid=int(ppid), pgid=int(pgid), state=state)
    return records


def owned_snapshot(pids, groups):
    table = process_table()
    selected = {pid for pid, record in table.items()
                if pid in pids or record["pgid"] in groups}
    while True:
        descendants = {pid for pid, record in table.items() if record["ppid"] in selected}
        expanded = selected | descendants
        if expanded == selected:
            break
        selected = expanded
    return [table[pid] for pid in sorted(selected)]


def assert_gone(pids, groups, timeout=5):
    deadline = time.monotonic() + timeout
    while True:
        remaining = owned_snapshot(pids, groups)
        if not remaining:
            return {"checked_pids": sorted(pids), "checked_pgids": sorted(groups),
                    "remaining": [], "zombies_count_as_remaining": True}
        if time.monotonic() >= deadline:
            raise AssertionError(f"owned processes remain after cleanup: {remaining}")
        time.sleep(.03)


class Evidence:
    def __init__(self, args, *, resume=False):
        self.args = args
        self.output = args.output.resolve()
        if not resume:
            require(not self.output.exists(), f"output already exists: {self.output}")
            self.output.mkdir(parents=True)
            self.owner = {"protocol": PROTOCOL, "run_id": uuid.uuid4().hex,
                          "script_sha256": digest(SCRIPT), "uid": os.getuid(),
                          "created_unix": time.time(), "output": str(self.output)}
            write_json(self.output / "owner.json", self.owner)
        else:
            require(self.output.is_dir(), "the owned output directory is missing")
            self.owner = json.loads((self.output / "owner.json").read_text())
            require(self.owner["protocol"] == PROTOCOL and self.owner["uid"] == os.getuid(),
                    "output belongs to a different protocol or user")
            require(self.owner["output"] == str(self.output), "output directory moved")
            require(self.owner["script_sha256"] == digest(SCRIPT), "probe source changed")
        self.report = {
            "protocol": PROTOCOL, "run_id": self.owner["run_id"], "mode": args.mode,
            "status": "running", "acceptance_level": args.acceptance_level,
            "build_label": args.build_label, "source_sha": args.source_sha,
            "source_sha_origin": "caller_supplied_build_provenance",
            "cli_source_sha": args.cli_source_sha or args.source_sha,
            "sdk_source_sha": args.sdk_source_sha or args.source_sha,
            "platform": platform.platform(), "machine": platform.machine(),
            "python": sys.version, "executable": sys.executable,
            "isolated": bool(sys.flags.isolated), "cwd": str(Path.cwd()),
            "script_sha256": digest(SCRIPT), "started_unix": time.time(),
            "checks": [], "commands": [],
        }
        self.path = self.output / f"{args.mode}-report.json"
        require(not self.path.exists(), "this probe mode already has a report")
        self.save()

    def save(self):
        write_json(self.path, self.report)

    def check(self, name, **evidence):
        self.report["checks"].append({"name": name, "status": "passed", **evidence})
        self.save()
        print(json.dumps({"event": "check_passed", "name": name,
                          "run_id": self.owner["run_id"]}), flush=True)

    def command(self, name, command, *, cwd=None, request=None, expected=0, timeout=20):
        path = self.output / f"{self.args.mode}-{len(self.report['commands']):03d}-{name}"
        result = subprocess.run(command, cwd=cwd or self.output,
                                input=None if request is None else json.dumps(request),
                                text=True, capture_output=True, timeout=timeout,
                                env=clean_environment())
        path.with_suffix(".stdout").write_text(result.stdout, encoding="utf-8")
        path.with_suffix(".stderr").write_text(result.stderr, encoding="utf-8")
        self.report["commands"].append({
            "name": name, "argv": list(map(str, command)),
            "cwd": str(cwd or self.output), "returncode": result.returncode,
            "stdout": str(path.with_suffix(".stdout").relative_to(self.output)),
            "stderr": str(path.with_suffix(".stderr").relative_to(self.output)),
        })
        self.save()
        require(result.returncode == expected,
                f"{name}: exit {result.returncode}, expected {expected}; "
                f"see {path.with_suffix('.stdout')} and .stderr")
        return result

    def finish(self, error=None):
        self.report.update(status="failed" if error else "passed", ended_unix=time.time())
        if error:
            self.report["failure"] = {"type": type(error).__name__, "message": str(error),
                                      "traceback": traceback.format_exc()}
        self.save()
        print(json.dumps({"event": "probe_finished", "status": self.report["status"],
                          "acceptance_level": self.args.acceptance_level,
                          "report": str(self.path)}), flush=True)


def installed_sdk(evidence):
    import aos_sdk as aos
    from aos_sdk import _native
    package = Path(aos.__file__).resolve()
    native = Path(_native.__file__).resolve()
    site = Path(sysconfig.get_paths()["platlib"]).resolve() / "aos_sdk"
    require(sys.flags.isolated, "use the installed environment's python -I")
    require(package.parent == native.parent and native.is_relative_to(site),
            "SDK did not come from this interpreter's installed site-packages")
    require(isinstance(_native.__loader__, importlib.machinery.ExtensionFileLoader),
            "native SDK must be a compiled extension")
    evidence.report["artifacts"] = {
        "sdk_version": aos.__version__, "sdk_package": str(package),
        "native_path": str(native), "native_sha256": digest(native),
        "cli_path": str(evidence.args.aos), "cli_sha256": digest(evidence.args.aos),
        "zenohd_path": str(evidence.args.zenohd),
        "zenohd_sha256": digest(evidence.args.zenohd),
        "installer_sha256": digest(ROOT / "scripts/install_demo_package.py"),
        "examples": {str(path.relative_to(ROOT)): digest(path)
                     for path in sorted((ROOT / "examples/launch").iterdir()) if path.is_file()},
    }
    if evidence.args.build_provenance:
        proof = evidence.args.build_provenance.resolve(strict=True)
        evidence.report["build_provenance"] = {"path": str(proof), "sha256": digest(proof)}
        verify_build(evidence, proof, package, native)
    else:
        require(evidence.args.acceptance_level != "final-artifacts",
                "final artifact acceptance requires --build-provenance")
    evidence.save()
    return aos


def verify_build(evidence, proof, package, native):
    record = json.loads(proof.read_text())
    args = evidence.args
    require(record["status"] == "passed", "native build acceptance did not pass")
    require(not record["tracked_changes"], "native build used tracked modifications")
    require(re.fullmatch(r"[0-9a-f]{40}", args.source_sha), "source SHA must be full length")
    require(record["source_sha"] == args.source_sha, "native build source SHA differs")
    if args.acceptance_level == "final-artifacts":
        require(evidence.report["cli_source_sha"] == args.source_sha
                and evidence.report["sdk_source_sha"] == args.source_sha,
                "final CLI, SDK and example sources must have the same source SHA")
    require(all(command["returncode"] == 0 for command in record["commands"]),
            "native acceptance contains an unsuccessful command")
    artifacts = evidence.report["artifacts"]
    require(record["cli"]["sha256"] == artifacts["cli_sha256"],
            "actual CLI differs from native build provenance")
    router_hash = record["zenohd"].get("executable_sha256", record["zenohd"].get("sha256"))
    require(router_hash == artifacts["zenohd_sha256"],
            "actual zenohd differs from native build provenance")
    wheel = proof.parent / record["wheel"]["path"]
    require(digest(wheel) == record["wheel"]["sha256"], "wheel hash differs from provenance")
    installed_files = {}
    with zipfile.ZipFile(wheel) as archive:
        for member in archive.namelist():
            if member.startswith("aos_sdk/") and not member.endswith("/"):
                installed = package.parent.parent / member
                expected = hashlib.sha256(archive.read(member)).hexdigest()
                require(digest(installed) == expected, f"installed SDK differs from wheel: {member}")
                installed_files[member] = expected
    require(f"aos_sdk/{native.name}" in installed_files, "native extension absent from wheel")
    source_paths = [
        ROOT / "scripts/install_demo_package.py", ROOT / "scripts/system_service.py",
        ROOT / "config/zenoh-session.json5", ROOT / "config/zenoh-router.json5",
        *sorted(path for path in (ROOT / "examples/launch").iterdir() if path.is_file()),
    ]
    source_hashes = {}
    for path in source_paths:
        relative = str(path.relative_to(ROOT))
        content = subprocess.run(
            ["git", "-C", str(ROOT), "show", f"{args.source_sha}:{relative}"],
            capture_output=True, timeout=10, check=True,
        ).stdout
        expected = hashlib.sha256(content).hexdigest()
        require(digest(path) == expected, f"working source differs from source SHA: {relative}")
        source_hashes[relative] = expected
    evidence.check("native-artifact-and-source-provenance",
                   native_build_status=record["status"], source_sha=record["source_sha"],
                   wheel_sha256=record["wheel"]["sha256"], sdk_files=installed_files,
                   source_files=source_hashes)


def prepare_network(evidence):
    network = evidence.output / "network"
    network.mkdir()
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    endpoint = f"tcp/127.0.0.1:{port}"
    for source, target in [("zenoh-router.json5", "router.json5"),
                           ("zenoh-session.json5", "session.json5")]:
        text = (ROOT / "config" / source).read_text()
        require(text.count("tcp/127.0.0.1:7447") == 1, "unexpected network template")
        (network / target).write_text(text.replace("tcp/127.0.0.1:7447", endpoint))
    return {"endpoint": endpoint, "port": port,
            "router": network / "router.json5", "session": network / "session.json5"}


def wait_port(process, port, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        require(process.poll() is None, f"router exited: {process.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=.1):
                return
        except OSError:
            time.sleep(.03)
    raise TimeoutError("owned loopback router did not listen")


class Router:
    def __init__(self, evidence, network):
        self.log = (evidence.output / "router.log").open("x")
        self.process = subprocess.Popen(
            [str(evidence.args.zenohd), "--config", str(network["router"])],
            stdout=self.log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
            env=clean_environment(), start_new_session=True,
        )
        try:
            wait_port(self.process, network["port"])
        except BaseException:
            self.close()
            raise

    def close(self):
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=3)
        self.log.close()
        return assert_gone({self.process.pid}, {self.process.pid})


def install_package(evidence, network, domain, namespace, suffix="installed"):
    prefix = evidence.output / suffix
    result = evidence.command(f"install-{suffix}", [
        sys.executable, "-I", str(ROOT / "scripts/install_demo_package.py"),
        "--prefix", str(prefix), "--config", str(network["session"]),
        "--domain-id", str(domain), "--namespace", namespace,
    ])
    installed = json.loads(result.stdout)
    require(installed["native_sha256"] == evidence.report["artifacts"]["native_sha256"],
            "installer imported another native SDK")
    if suffix == "installed":
        (prefix / "bin").mkdir()
        shutil.copy2(evidence.args.aos, prefix / "bin/aos")
    return prefix, installed


def deployment(evidence, prefix, namespace, *, always=False):
    plan = json.loads((prefix / "share/aos_demo/launch/pair.json").read_text())
    for process in plan["processes"]:
        declared = "worker" if process["id"] == "provider" else "consumer"
        name = "managed" if process["id"] == "provider" else "observer"
        process["node"]["nodes"][declared].update(name=name, namespace=namespace)
        process["readiness"]["node"] = f"{namespace}/{name}"
        for endpoint in process["readiness"].get("endpoints", []):
            endpoint["name"] = f"{namespace}/{Path(endpoint['name']).name}"
        process["env"] = {"AOS_USE_SIM_TIME": "false", "AOS_NODE_NAME": "shadowed_environment"}
        if process["id"] == "provider":
            if always:
                process["restart"]["policy"] = "always"
        else:
            process["args"] = ["--labels", "7"]
    request = {
        "prefixes": [str(prefix)],
        "path": str(evidence.output / f"deployment-{'manual' if always else 'restart'}.json"),
        "overrides": {"nodes": {"provider": {"nodes": {
            "worker": {"parameters": {"threshold": 7}},
        }}}},
    }
    write_json(Path(request["path"]), plan)
    return plan, request


def cli(evidence, binary, name, group, verb, request, expected=0):
    return evidence.command(name, [str(binary), group, verb, "--request", "-"],
                            cwd=evidence.output / "unrelated-cwd", request=request,
                            expected=expected, timeout=20)


class Launch:
    def __init__(self, evidence, binary, name, request):
        self.evidence = evidence
        self.name = name
        self.records = []
        self.failure = None
        self.condition = threading.Condition()
        self.stdout = (evidence.output / f"{name}.stdout.jsonl").open("x")
        self.stderr = (evidence.output / f"{name}.stderr.jsonl").open("x")
        request_file = evidence.output / f"{name}.request.json"
        write_json(request_file, request)
        self.process = subprocess.Popen(
            [str(binary), "launch", "start", "--request", str(request_file)],
            cwd=evidence.output / "unrelated-cwd", stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=self.stderr, text=True,
            env=clean_environment(), start_new_session=True,
        )
        self.reader = threading.Thread(target=self.read, name=f"{name}-jsonl", daemon=True)
        self.reader.start()

    def read(self):
        try:
            for line in self.process.stdout:
                self.stdout.write(line)
                self.stdout.flush()
                record = json.loads(line)
                with self.condition:
                    require(len(self.records) < 20000, "probe event bound exceeded")
                    self.records.append(record)
                    self.condition.notify_all()
        except BaseException as error:
            self.failure = error
        finally:
            with self.condition:
                self.condition.notify_all()

    def snapshot(self):
        with self.condition:
            if self.failure:
                raise RuntimeError(f"invalid launch output: {self.failure}")
            return list(self.records)

    def wait_event(self, event, process_id=None, generation=None, timeout=20):
        deadline = time.monotonic() + timeout
        while True:
            for record in self.snapshot():
                if (record.get("event") == event
                        and (process_id is None or record.get("process_id") == process_id)
                        and (generation is None or record.get("generation") == generation)):
                    return record
            require(self.process.poll() is None,
                    f"{self.name} exited before {event}/{process_id}/{generation}: "
                    f"{self.process.returncode}; {self.snapshot()[-3:]}")
            if time.monotonic() >= deadline:
                raise TimeoutError(f"{self.name}: missing {event}/{process_id}/{generation}")
            with self.condition:
                self.condition.wait(.05)

    def identities(self):
        records = self.snapshot()
        pids = {self.process.pid}
        groups = {self.process.pid}
        for record in records:
            if record.get("event") == "process_started":
                pids.add(record["pid"])
                groups.add(record["pgid"])
        return pids, groups

    def finish(self, expected, timeout=20):
        self.process.wait(timeout=timeout)
        self.reader.join(timeout=3)
        require(not self.reader.is_alive(), "launch output reader did not finish")
        self.stdout.close()
        self.stderr.close()
        self.process.stdout.close()
        records = self.snapshot()
        require(self.process.returncode == expected,
                f"{self.name}: exit {self.process.returncode}, expected {expected}")
        summary = [record["data"] for record in records if record["event"] == "summary"]
        require(len(summary) == 1, f"{self.name}: expected one terminal summary")
        require(summary[0]["cleanup_errors"] == [], f"cleanup errors: {summary[0]}")
        gone = assert_gone(*self.identities())
        return {"exit": self.process.returncode, "summary": summary[0], "cleanup": gone}

    def stop_after_failure(self):
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=55)
            except subprocess.TimeoutExpired as error:
                # Never kill only the supervisor and silently orphan its groups.
                raise RuntimeError(
                    f"owned supervisor {self.process.pid} did not clean up; "
                    f"manual inspection required, identities={self.identities()}"
                ) from error
        self.reader.join(timeout=3)
        self.stdout.close()
        self.stderr.close()
        if self.process.stdout:
            self.process.stdout.close()
        return assert_gone(*self.identities())


class StatusObserver:
    def __init__(self, aos, prefix, network, domain, namespace):
        self.values = deque(maxlen=4000)
        self.condition = threading.Condition()
        interface = aos.Interface.load(prefix / "share/aos_demo/interfaces/Status.json")
        self.context = aos.Context(config=network["session"], domain_id=domain)
        self.node = aos.Node("acceptance_observer", f"{namespace}/probe", context=self.context)

        def receive(value):
            with self.condition:
                self.values.append((time.monotonic(), value))
                self.condition.notify_all()

        self.subscription = self.node.create_subscription(interface, f"{namespace}/status", receive)
        self.executor = aos.MultiThreadedExecutor(2, context=self.context)
        self.executor.add_node(self.node)
        self.spinner = threading.Thread(target=self.executor.spin, daemon=True)
        self.spinner.start()

    def wait(self, pid, count, state, samples=3, after=0, timeout=15):
        deadline = time.monotonic() + timeout
        while True:
            with self.condition:
                values = [value for when, value in self.values
                          if when >= after and value["pid"] == pid]
                expected = [value for value in values
                            if value["received"] == count and value["state"] == state
                            and value["threshold"] == 7]
                if len(expected) >= samples:
                    return expected[-samples:]
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"business status mismatch for PID {pid}: {values[-5:]}")
                self.condition.wait(.05)

    def close(self):
        stopped = self.executor.shutdown(timeout_sec=5)
        self.spinner.join(timeout=2)
        self.node.destroy_node()
        self.context.shutdown()
        require(stopped and not self.spinner.is_alive(), "status observer failed to stop")


def node_card(evidence, binary, network, domain, name, expected, label):
    result = cli(evidence, binary, label, "node", "info", {
        "node_name": name, "timeout": 8,
        "context_config": {"cli": {"domain_id": domain, "zenoh_config": str(network["session"])}},
    })
    card = json.loads(result.stdout)["data"]
    require(card["fully_qualified_name"] == name, "NodeCard FQN mismatch")
    require(card["process"] == {key: expected[key]
                               for key in ("launch_id", "process_id", "generation", "pid")},
            f"NodeCard generation ownership mismatch: {card['process']}")
    effective = card["effective_config"]
    require(effective["name"] == {"value": Path(name).name, "source": "launch"},
            f"renamed Node not applied: {effective['name']}")
    require(effective["namespace"] == {"value": str(Path(name).parent), "source": "launch"},
            "launch namespace not applied")
    require(effective["use_sim_time"] == {"value": False, "source": "environment"},
            "OS environment did not enter the standard SDK configuration")
    if expected["process_id"] == "provider":
        threshold = effective["parameters"]["threshold"]
        require(threshold["value"] == {"type": "integer", "value": 7}
                and threshold["source"] == "cli", "threshold override not applied in NodeCard")
        require(card["lifecycle"]["current_state"]["label"] == "active",
                f"provider is not active: {card['lifecycle']}")
    return card


def assert_no_replay(run, initial_consumer_pid):
    records = run.snapshot()
    batches = [record for record in records if record.get("event") == "process_stdout"
               and record["details"]["value"].get("event") == "batch_submitted"]
    require(len(batches) == 1, f"consumer replayed a business batch: {len(batches)}")
    require(batches[0]["details"]["value"]["count"] == 7
            and batches[0]["pid"] == initial_consumer_pid, "wrong consumer batch")
    starts = [record for record in records if record.get("event") == "process_started"
              and record["process_id"] == "consumer"]
    require(len(starts) == 1 and starts[0]["pid"] == initial_consumer_pid,
            "consumer was implicitly restarted")
    return {"batch_submissions": 1, "submitted_labels": 7,
            "consumer_pid": initial_consumer_pid, "consumer_generations": 1}


def negative_preflight(evidence, binary, plan, prefix, duplicate):
    missing = deepcopy(plan)
    missing["processes"].append({"id": "missing", "package": "absent", "entry": "entry"})
    bad_entry = deepcopy(plan)
    bad_entry["processes"].append({"id": "bad-entry", "package": "aos_demo", "entry": "absent"})
    bad_dependency = deepcopy(plan)
    bad_dependency["processes"][1]["depends_on"] = ["absent"]
    cases = [
        ("missing-package", {"prefixes": [str(prefix)], "plan": missing}, 3, "PACKAGE_NOT_FOUND"),
        ("ambiguous-package", {"prefixes": [str(prefix), str(duplicate)], "plan": plan},
         3, "AMBIGUOUS_PACKAGE"),
        ("missing-entry", {"prefixes": [str(prefix)], "plan": bad_entry}, 3, "ENTRY_NOT_FOUND"),
        ("invalid-dependency", {"prefixes": [str(prefix)], "plan": bad_dependency},
         2, "INVALID_ARGUMENT"),
    ]
    for name, request, status, code in cases:
        result = cli(evidence, binary, name, "launch", "start", request, expected=status)
        records = [json.loads(line) for line in result.stdout.splitlines()]
        require(not any(record["event"] == "process_started" for record in records),
                f"{name} started a child before completing preflight")
        require(records[-1]["event"] == "error" and records[-1]["data"]["code"] == code,
                f"wrong preflight error: {records}")
        evidence.check(f"preflight-{name}", exit=status, code=code,
                       process_started_events=0, terminal_error=records[-1])


def local(evidence, aos):
    args = evidence.args
    network = prepare_network(evidence)
    namespace = f"/stage4_{evidence.owner['run_id'][:12]}"
    domain = args.domain_id
    (evidence.output / "unrelated-cwd").mkdir()
    version = evidence.command("zenohd-version", [str(args.zenohd), "--version"])
    require(re.search(r"\bzenohd v?1\.10\.1\b", version.stdout), "require zenohd 1.10.1")
    router = Router(evidence, network)
    observer = None
    manual_observer = None
    runs = []
    try:
        prefix, installed = install_package(evidence, network, domain, namespace)
        duplicate, _ = install_package(evidence, network, domain, namespace, "ambiguous-prefix")
        binary = prefix / "bin/aos"
        require(digest(binary) == evidence.report["artifacts"]["cli_sha256"],
                "installed CLI differs from the supplied artifact")
        package = json.loads(cli(evidence, binary, "installed-package-info", "package", "info",
                                 {"package": "aos_demo"}).stdout)["data"]
        resource = json.loads(cli(evidence, binary, "installed-package-resource", "package", "resource",
                                  {"package": "aos_demo", "resource": "pair"}).stdout)["data"]
        require(Path(package["prefix"]) == prefix, "installed CLI did not find its own prefix")
        require(Path(resource["path"]) == prefix / "share/aos_demo/launch/pair.json",
                "package resource resolved outside the installed prefix")
        evidence.check("installed-package-from-arbitrary-cwd", package=package,
                       resource=resource, installation=installed)
        plan, request = deployment(evidence, prefix, namespace)
        negative_preflight(evidence, binary, plan, prefix, duplicate)
        observer = StatusObserver(aos, prefix, network, domain, namespace)
        run = Launch(evidence, binary, "finite-restart", request)
        runs.append(run)
        provider = run.wait_event("ready", "provider", 1)
        consumer = run.wait_event("ready", "consumer", 1)
        records = run.snapshot()
        consumer_start = next(record for record in records
                              if record["event"] == "process_started" and record["process_id"] == "consumer")
        require(records.index(provider) < records.index(consumer_start),
                "consumer started before provider readiness")
        initial = observer.wait(provider["pid"], 7, "ready")
        cards = {
            "provider": node_card(evidence, binary, network, domain, f"{namespace}/managed",
                                  provider, "provider-card-generation-1"),
            "consumer": node_card(evidence, binary, network, domain, f"{namespace}/observer",
                                  consumer, "consumer-card"),
        }
        evidence.check("real-installed-program-injection-and-business", node_cards=cards,
                       business_samples=initial, provider_ready_before_consumer_start=True,
                       owned_processes=owned_snapshot(*run.identities()))
        for generation in (1, 2, 3):
            identity = run.wait_event("ready", "provider", generation)
            table = process_table()
            require(identity["pid"] in table
                    and table[identity["pid"]]["ppid"] == run.process.pid
                    and table[identity["pid"]]["pgid"] == identity["pgid"],
                    "refusing to signal a process not owned by this supervisor")
            os.kill(identity["pid"], signal.SIGKILL)
            run.wait_event("process_exited", "provider", generation)
            if generation < 3:
                replacement = run.wait_event("ready", "provider", generation + 1)
                require(replacement["pid"] != identity["pid"], "provider PID did not change")
                card = node_card(evidence, binary, network, domain, f"{namespace}/managed",
                                 replacement, f"provider-card-generation-{generation + 1}")
                # Read new messages after the process is ready; do not rely on a
                # retained zero counter or consumer log from an earlier process.
                after = time.monotonic()
                samples = observer.wait(replacement["pid"], 0, "waiting",
                                        samples=12, after=after)
                no_replay = assert_no_replay(run, consumer["pid"])
                evidence.check(f"provider-restart-generation-{generation + 1}",
                               killed=identity, replacement=card,
                               fresh_business_samples=samples, **no_replay)
        run.wait_event("restart_exhausted", "provider", 3)
        result = run.finish(1)
        summary = result["summary"]
        provider_summary = next(item for item in summary["processes"] if item["process_id"] == "provider")
        require(provider_summary["generations"] == 3 and provider_summary["restarts"] == 2,
                "finite restart budget was not enforced")
        require(sum(record["event"] == "restart_scheduled" for record in run.snapshot()) == 2,
                "unexpected scheduled restart count")
        evidence.check("restart-budget-exhaustion-cleans-all-owned-groups",
                       result=result, **assert_no_replay(run, consumer["pid"]))
        observer.close()
        observer = None
        manual_namespace = namespace + "_manual"
        _, manual_request = deployment(evidence, prefix, manual_namespace, always=True)
        manual_observer = StatusObserver(aos, prefix, network, domain, manual_namespace)
        manual = Launch(evidence, binary, "explicit-stop", manual_request)
        runs.append(manual)
        provider = manual.wait_event("ready", "provider", 1)
        consumer = manual.wait_event("ready", "consumer", 1)
        samples = manual_observer.wait(provider["pid"], 7, "ready")
        before = owned_snapshot(*manual.identities())
        manual.process.send_signal(signal.SIGTERM)
        result = manual.finish(143)
        require(result["summary"]["stop_reason"] == {"kind": "signal", "detail": signal.SIGTERM},
                "explicit stop lost its signal identity")
        require(not any(record["event"] == "restart_scheduled" for record in manual.snapshot()),
                "explicit stop caused an automatic restart")
        evidence.check("explicit-stop-overrides-always-restart", business_samples=samples,
                       before=before, result=result, **assert_no_replay(manual, consumer["pid"]))
    finally:
        original_failure = sys.exc_info()[1]
        cleanup = []
        for run in runs:
            try:
                cleanup.append({"name": run.name, "result": run.stop_after_failure()})
            except BaseException as error:
                cleanup.append({"name": run.name, "error": str(error)})
        for name, item in (("business-observer", observer), ("manual-observer", manual_observer)):
            if item:
                try:
                    item.close()
                    cleanup.append({"name": name, "result": "stopped"})
                except BaseException as error:
                    cleanup.append({"name": name, "error": str(error)})
        try:
            cleanup.append({"name": "owned-loopback-router", "result": router.close()})
        except BaseException as error:
            cleanup.append({"name": "owned-loopback-router", "error": str(error)})
        evidence.report["final_cleanup"] = cleanup
        evidence.save()
        errors = [item for item in cleanup if "error" in item]
        if errors:
            raise RuntimeError(f"final cleanup failed: {errors}") from original_failure


def service(evidence, action, name=None, *options):
    result = evidence.command(
        f"service-{action}",
        [sys.executable, "-I", str(ROOT / "scripts/system_service.py"), action,
         *([] if name is None else ["--name", name]), *map(str, options)],
        timeout=80 if action in ("stop", "uninstall") else 20,
    )
    value = json.loads(result.stdout)
    require(value["ok"], f"system service helper failed: {value}")
    return value["data"]


def process_start_ticks(pid):
    try:
        # comm can contain spaces; starttime is field 22 after the closing ')'.
        return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19]
    except FileNotFoundError:
        return None


def ssh_identity():
    connection = os.environ.get("SSH_CONNECTION")
    require(connection, "run this phase through its own SSH connection")
    parent = os.getppid()
    return {"ssh_connection_sha256": hashlib.sha256(connection.encode()).hexdigest(),
            "parent_pid": parent, "parent_start_ticks": process_start_ticks(parent),
            "session_id": os.environ.get("XDG_SESSION_ID"),
            "captured_unix": time.time()}


def systemd_launch_records(evidence, name):
    journal = service(evidence, "logs", name, "--lines", "1000")
    records = []
    for record in journal["records"]:
        try:
            value = json.loads(record.get("MESSAGE", ""))
        except (TypeError, ValueError):
            continue
        if isinstance(value, dict) and "event" in value and "schema_version" in value:
            records.append(value)
    return records


def wait_systemd_ready(evidence, name, timeout=25):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        records = systemd_launch_records(evidence, name)
        ready = {record["process_id"]: record for record in records
                 if record["event"] == "ready"}
        if "provider" in ready and "consumer" in ready:
            return records, ready
        require(not any(record["event"] in ("error", "summary") for record in records),
                f"service exited before both installed programs were ready: {records[-5:]}")
        time.sleep(.25)
    raise TimeoutError("installed systemd programs did not become ready")


def make_systemd_wrapper(evidence, binary, network):
    """The wrapper execs real aos. Its router belongs to the systemd cgroup."""
    manifest = {
        "protocol": PROTOCOL, "run_id": evidence.owner["run_id"],
        "output": str(evidence.output), "uid": os.getuid(),
        "binary": str(binary), "binary_sha256": digest(binary),
        "zenohd": str(evidence.args.zenohd), "zenohd_sha256": digest(evidence.args.zenohd),
        "router_config": str(network["router"]), "port": network["port"],
    }
    manifest_path = evidence.output / "systemd-wrapper.json"
    write_json(manifest_path, manifest)
    launcher = evidence.output / "systemd-wrapper.py"
    launcher.write_text(
        "import hashlib,json,os,pathlib,socket,subprocess,sys,time\n"
        f"manifest=json.loads(pathlib.Path({str(manifest_path)!r}).read_text())\n"
        "output=pathlib.Path(manifest['output'])\n"
        "owner=json.loads((output/'owner.json').read_text())\n"
        "assert owner['run_id']==manifest['run_id'] and owner['uid']==os.getuid()\n"
        "assert owner['protocol']==manifest['protocol']\n"
        "for key in ('binary','zenohd'):\n"
        "    actual=hashlib.sha256(pathlib.Path(manifest[key]).read_bytes()).hexdigest()\n"
        "    assert actual==manifest[key+'_sha256']\n"
        "assert sys.argv[1:3]==['launch','start']\n"
        "env={k:v for k,v in os.environ.items() if not k.startswith('AOS_') "
        "and k not in ('PYTHONPATH','PYTHONHOME')}\n"
        "log=(output/'systemd-router.log').open('a')\n"
        "router=subprocess.Popen([manifest['zenohd'],'--config',manifest['router_config']],"
        "stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,"
        "env=env,start_new_session=True)\n"
        "identity={'wrapper_pid':os.getpid(),'router_pid':router.pid,"
        "'actual_aos':manifest['binary'],'aos_sha256':manifest['binary_sha256'],"
        "'router_cleanup_owner':'systemd_cgroup','created_unix':time.time()}\n"
        "(output/'systemd-processes.json').write_text(json.dumps(identity,indent=2)+'\\n')\n"
        "deadline=time.monotonic()+10\n"
        "while True:\n"
        "    assert router.poll() is None,'router exited'\n"
        "    try:\n"
        "        with socket.create_connection(('127.0.0.1',manifest['port']),timeout=.1):\n"
        "            break\n"
        "    except OSError:\n"
        "        if time.monotonic()>=deadline:\n"
        "            raise TimeoutError('owned router did not listen')\n"
        "        time.sleep(.03)\n"
        "log.close()\n"
        "os.execve(manifest['binary'],[manifest['binary'],*sys.argv[1:]],env)\n"
    )
    wrapper = evidence.output / "systemd-entry"
    wrapper.write_text("#!/bin/sh\nexec " + shlex.quote(sys.executable) + " -I "
                       + shlex.quote(str(launcher)) + ' "$@"\n')
    wrapper.chmod(0o700)
    return wrapper


def assert_systemd_owned(context):
    state = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local/state"))
    owner_path = state / "aos/system-services" / context["unit"] / "owner.json"
    record = json.loads(owner_path.read_text())
    require(record["uid"] == os.getuid() and record["unit"] == context["unit"],
            "service ownership record has another identity")
    require(record["aos"] == context["wrapper"] and record["request"] == context["request"],
            "refusing to change a unit not created by this exact probe")
    return owner_path


def cleanup_systemd(evidence, context):
    assert_systemd_owned(context)
    records = []
    try:
        stopped = service(evidence, "stop", context["name"])
        deadline = time.monotonic() + 3
        while True:
            records = systemd_launch_records(evidence, context["name"])
            if any(record["event"] == "summary" for record in records) or time.monotonic() >= deadline:
                break
            time.sleep(.1)
    finally:
        # Even a failed stop or missing final journal record must attempt to
        # remove this exact owned unit. The helper rechecks ownership itself.
        removed = service(evidence, "uninstall", context["name"])
    identities = context.get("processes")
    if identities is None and (evidence.output / "systemd-processes.json").exists():
        identities = json.loads((evidence.output / "systemd-processes.json").read_text())
    identities = identities or {}
    starts = [record for record in records if record["event"] == "process_started"]
    pids = set(context.get("child_pids", [])) | {record["pid"] for record in starts}
    groups = set(context.get("child_pgids", [])) | {record["pgid"] for record in starts}
    for key in ("wrapper_pid", "router_pid"):
        if key in identities:
            pids.add(identities[key])
            groups.add(identities[key])
    gone = assert_gone(pids, groups)
    summaries = [record["data"] for record in records if record["event"] == "summary"]
    result = {"stop": stopped, "uninstall": removed, "process_cleanup": gone,
              "aos_terminal_summaries": summaries, "router_cleanup_owner": "systemd_cgroup",
              "router_graceful_cleanup_claimed": False}
    evidence.report["systemd_cleanup_observed"] = result
    evidence.save()
    require(stopped["remaining_pids"] == [], "systemd cgroup still contains processes")
    require(removed["removed"] and removed["remaining_pids"] == []
            and removed["load_state"] == "not-found", "owned unit was not fully removed")
    require(len(summaries) == 1 and summaries[0]["cleanup_errors"] == [],
            "the actual AOS supervisor did not report successful child cleanup")
    require(summaries[0]["stop_reason"] == {"kind": "signal", "detail": signal.SIGTERM},
            "systemd did not stop the AOS supervisor through SIGTERM")
    require(not any(record["event"] == "restart_scheduled" for record in records),
            "explicit systemd stop caused a child restart")
    return result


def systemd_start(evidence, aos):
    require(sys.platform == "linux", "systemd acceptance requires Linux")
    connection = ssh_identity()
    audit = service(evidence, "audit")
    require(audit["user_manager"] in ("running", "degraded"), "user manager is unavailable")
    network = prepare_network(evidence)
    namespace = f"/stage4_systemd_{evidence.owner['run_id'][:12]}"
    domain = evidence.args.domain_id
    (evidence.output / "unrelated-cwd").mkdir()
    prefix, installed = install_package(evidence, network, domain, namespace)
    binary = prefix / "bin/aos"
    _, request = deployment(evidence, prefix, namespace)
    request_path = evidence.output / "systemd-launch.request.json"
    write_json(request_path, request)
    wrapper = make_systemd_wrapper(evidence, binary, network)
    name = "stage4-program-" + evidence.owner["run_id"][:20]
    context = {"name": name, "unit": f"aos-{name}.service", "wrapper": str(wrapper),
               "request": str(request_path), "prefix": str(prefix), "binary": str(binary),
               "namespace": namespace, "domain": domain,
               "network": {key: str(value) if isinstance(value, Path) else value
                           for key, value in network.items()},
               "source_sha": evidence.args.source_sha, "connection_before": connection,
               "audit_before": audit, "router_cleanup_owner": "systemd_cgroup"}
    context_path = evidence.output / "systemd-context.json"
    write_json(context_path, context)
    observer = None
    try:
        installed_unit = service(
            evidence, "install", name, "--aos", str(wrapper), "--request", str(request_path),
            "--working-directory", str(evidence.output / "unrelated-cwd"), "--start",
        )
        records, ready = wait_systemd_ready(evidence, name)
        status = service(evidence, "status", name)
        main_pid = int(status["systemd"]["MainPID"])
        identity = json.loads((evidence.output / "systemd-processes.json").read_text())
        require(main_pid == identity["wrapper_pid"], "wrapper did not exec into MainPID")
        require(Path(f"/proc/{main_pid}/exe").resolve() == binary,
                "MainPID is not the actual installed aos binary")
        require(status["systemd"]["ActiveState"] == "active"
                and status["systemd"]["NRestarts"] == "0", "service restarted or is inactive")
        observer = StatusObserver(aos, prefix, network, domain, namespace)
        samples = observer.wait(ready["provider"]["pid"], 7, "ready", after=time.monotonic())
        cards = {kind: node_card(evidence, binary, network, domain,
                                f"{namespace}/{'managed' if kind == 'provider' else 'observer'}",
                                ready[kind], f"systemd-before-{kind}-card")
                 for kind in ("provider", "consumer")}
        observer.close()
        observer = None
        starts = [record for record in records if record["event"] == "process_started"]
        context.update(processes=identity, ready=ready, business_samples_before=samples,
                       status_before=status, main_pid=main_pid,
                       main_start_ticks=process_start_ticks(main_pid),
                       child_pids=[record["pid"] for record in starts],
                       child_pgids=[record["pgid"] for record in starts])
        write_json(context_path, context)
        evidence.check("systemd-real-program-before-control-ssh-exit",
                       installed=installed, installed_unit=installed_unit, status=status,
                       actual_main_executable=str(Path(f"/proc/{main_pid}/exe").resolve()),
                       process_identity=identity, node_cards=cards,
                       fresh_business_samples=samples, connection=connection, audit=audit)
        evidence.report["phase_status"] = "awaiting_fresh_ssh_check"
        evidence.report["boundary"] = {
            "requires_second_ssh": True, "last_login_logout_tested": False,
            "linger_modified": False, "other_sessions_stopped": False,
            "router_cleanup_owner": "systemd_cgroup",
        }
        evidence.save()
    except BaseException:
        try:
            evidence.report["failure_cleanup"] = cleanup_systemd(evidence, context)
        except BaseException as cleanup_error:
            evidence.report["failure_cleanup"] = {"error": str(cleanup_error)}
        evidence.save()
        raise
    finally:
        if observer:
            observer.close()


def systemd_check_stop(evidence, aos):
    require(sys.platform == "linux", "systemd acceptance requires Linux")
    context = json.loads((evidence.output / "systemd-context.json").read_text())
    require(context["source_sha"] == evidence.args.source_sha, "source changed between SSH phases")
    observer = None
    try:
        connection = ssh_identity()
        previous = context["connection_before"]
        require(connection["ssh_connection_sha256"] != previous["ssh_connection_sha256"],
                "this is not a fresh SSH connection")
        require(process_start_ticks(previous["parent_pid"]) != previous["parent_start_ticks"],
                "the previous controlling SSH command is still alive")
        assert_systemd_owned(context)
        status = service(evidence, "status", context["name"])
        require(status["systemd"]["ActiveState"] == "active"
                and int(status["systemd"]["MainPID"]) == context["main_pid"]
                and status["systemd"]["NRestarts"] == "0",
                "systemd did not retain the same service process after SSH exit")
        require(process_start_ticks(context["main_pid"]) == context["main_start_ticks"],
                "the saved MainPID was reused by another process")
        require(Path(f"/proc/{context['main_pid']}/exe").resolve() == Path(context["binary"]),
                "MainPID no longer runs installed aos")
        network = {key: Path(value) if key in ("router", "session") else value
                   for key, value in context["network"].items()}
        observer = StatusObserver(aos, Path(context["prefix"]), network,
                                  context["domain"], context["namespace"])
        samples = observer.wait(context["ready"]["provider"]["pid"], 7, "ready",
                                after=time.monotonic())
        require(samples[0]["sequence"] > context["business_samples_before"][-1]["sequence"],
                "business messages did not advance across the SSH boundary")
        cards = {kind: node_card(
            evidence, Path(context["binary"]), network, context["domain"],
            f"{context['namespace']}/{'managed' if kind == 'provider' else 'observer'}",
            context["ready"][kind], f"systemd-after-{kind}-card",
        ) for kind in ("provider", "consumer")}
        audit = service(evidence, "audit")
        require(audit["login"].get("Linger") == context["audit_before"]["login"].get("Linger"),
                "linger state changed during acceptance")
        evidence.check("same-systemd-program-after-control-ssh-disconnection",
                       connection_before=previous, connection_after=connection, status=status,
                       node_cards=cards, fresh_business_samples=samples,
                       sequence_before=context["business_samples_before"][-1]["sequence"],
                       sequence_after=samples[0]["sequence"], audit_before=context["audit_before"],
                       audit_after=audit, last_login_logout_tested=False,
                       linger_modified=False, other_sessions_stopped=False)
    finally:
        original_failure = sys.exc_info()[1]
        failures = []
        if observer:
            try:
                observer.close()
            except BaseException as error:
                failures.append(str(error))
        try:
            result = cleanup_systemd(evidence, context)
            evidence.check("systemd-owned-cgroup-and-unit-cleanup", **result)
        except BaseException as error:
            failures.append(str(error))
        evidence.report["final_cleanup_errors"] = failures
        evidence.save()
        if failures:
            raise RuntimeError(f"systemd cleanup failed: {failures}") from original_failure


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("mode", choices=["local", "systemd-start", "systemd-check-stop"])
    result.add_argument("--output", type=Path, required=True)
    result.add_argument("--aos", type=Path, required=True)
    result.add_argument("--zenohd", type=Path, required=True)
    result.add_argument("--source-sha", required=True)
    result.add_argument("--cli-source-sha")
    result.add_argument("--sdk-source-sha")
    result.add_argument("--build-label", required=True)
    result.add_argument("--build-provenance", type=Path)
    result.add_argument("--acceptance-level", choices=["candidate", "final-artifacts"], default="candidate")
    result.add_argument("--domain-id", type=int, default=74113)
    return result


def main():
    args = parser().parse_args()
    require(os.name == "posix", "launch program acceptance requires macOS or Linux")
    require(0 <= args.domain_id <= 2**32 - 1, "domain-id must be u32")
    args.aos = args.aos.resolve(strict=True)
    args.zenohd = args.zenohd.resolve(strict=True)
    require(args.aos.is_file() and os.access(args.aos, os.X_OK), "aos is not executable")
    require(args.zenohd.is_file() and os.access(args.zenohd, os.X_OK), "zenohd is not executable")
    require(args.output.is_absolute(), "output must be absolute")
    require(not Path.cwd().resolve().is_relative_to(ROOT),
            "run from an unrelated working directory outside the source checkout")
    for name in list(os.environ):
        if name.startswith("AOS_"):
            os.environ.pop(name)
    evidence = Evidence(args, resume=args.mode == "systemd-check-stop")
    try:
        aos = installed_sdk(evidence)
        {"local": local, "systemd-start": systemd_start,
         "systemd-check-stop": systemd_check_stop}[args.mode](evidence, aos)
    except BaseException as error:
        evidence.finish(error)
        return 1
    evidence.finish()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
