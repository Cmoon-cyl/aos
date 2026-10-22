#!/usr/bin/env python3
"""Audit mixed-version installed CLIs against real, independently logged providers.

Run with CPython 3.11 -I. Reuses verified environments without installing or
editing them. Only the new output directory and owned child processes change.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import traceback
import uuid

SCRIPT = Path(__file__).resolve()
PROTOCOL = "aos-platform-cli-compatibility/1"


def sibling(name):
    spec = importlib.util.spec_from_file_location(name, SCRIPT.with_name(name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


compat = sibling("platform_compatibility_probe")
probe = sibling("platform_probe")
require, digest, load, save = compat.require, compat.digest, compat.load, compat.save
DEFINITIONS = {
    **probe.DEFINITIONS,
    "ingress": {
        "name": "aos_probe/msg/Command", "kind": "msg",
        "message": probe.object_schema(**probe.REQUEST, value=probe.INTEGER),
    },
    "receipt": {
        "name": "aos_probe/srv/Receipt", "kind": "srv",
        "request": probe.object_schema(**probe.REQUEST, topic_request_id=probe.STRING),
        "response": probe.object_schema(
            origin=probe.ORIGIN, request_id=probe.STRING, topic_request_id=probe.STRING,
            received_count=probe.INTEGER, input_value=probe.INTEGER, applied_value=probe.INTEGER,
        ),
    },
}
COMMANDS = (
    ("node", "list"), ("node", "info"),
    ("topic", "list"), ("topic", "info"), ("topic", "type"),
    ("topic", "echo"), ("topic", "pub"),
    ("service", "list"), ("service", "type"), ("service", "call"),
    ("action", "list"), ("action", "info"), ("action", "type"),
    ("action", "send_goal"), ("action", "cancel"), ("action", "get_result"),
    ("interface", "show"),
)
SOURCE_FILES = (SCRIPT, SCRIPT.with_name("platform_probe.py"),
                SCRIPT.with_name("platform_compatibility_probe.py"))


def rows(path, *, partial=False):
    if partial and not path.exists():
        return []
    raw = path.read_text()
    if partial and raw and not raw.endswith("\n"):
        raw = raw.rsplit("\n", 1)[0] if "\n" in raw else ""
    return [json.loads(line) for line in raw.splitlines() if line]


def one(values, description):
    require(len(values) == 1, f"{description}: expected exactly one, found {len(values)}")
    return values[0]


def event(values, name):
    return one([row for row in values if row.get("event") == name], name)


def response(values):
    value = one(values, "JSON response")
    require(value.get("ok") is True, f"CLI response is not successful: {value}")
    return value["data"]


def provider_worker(args):
    build = load(args.build)
    require(compat.installed_binding(build) == build["installed"], "provider installed bytes differ")
    args.mode, args.round = "provider", 1
    args.source_sha = build["source_sha"]
    args.artifact_sha256 = build["artifact_sha256"]["wheel"]
    args.build_label = "installed mixed-version CLI provider"
    args.network_label = "owned single-host loopback TCP router"
    args.output.mkdir()
    evidence = probe.Evidence(args)
    evidence.base.update(worker_sha256=digest(SCRIPT),
                         compatibility_helper_sha256=digest(SOURCE_FILES[2]))
    try:
        with probe.runtime(args, evidence, "cli_provider") as (aos, _, node, _, stopping):
            interfaces = {key: aos.Interface.load(value) for key, value in DEFINITIONS.items()}
            origin = dict(run_id=args.run_id, instance_id=uuid.uuid4().hex, round=1,
                          pid=os.getpid(), hostname=socket.gethostname(),
                          node_id=node.get_node_card()["id"], source_sha=args.source_sha,
                          native_sha256=evidence.base["native_sha256"],
                          probe_sha256=evidence.base["probe_sha256"])
            ingress = {}
            changed = threading.Condition()

            def validate(request):
                require(request["run_id"] == args.run_id
                        and request["instance_id"] == origin["instance_id"], "stale CLI request")

            def receive(request):
                validate(request)
                with changed:
                    record = ingress.setdefault(request["request_id"], {"count": 0, "request": request})
                    require(record["request"] == request, "one Topic ID used for different values")
                    record["count"] += 1
                    evidence.emit("ingress_applied", request=request, count=record["count"],
                                  applied_value=request["value"] * 7)
                    changed.notify_all()

            def receipt(request):
                validate(request)
                deadline = time.monotonic() + 3
                with changed:
                    while request["topic_request_id"] not in ingress:
                        remaining = deadline - time.monotonic()
                        require(remaining > 0, "provider never received CLI publication")
                        changed.wait(remaining)
                    # A bounded quiet window catches immediate repeated delivery.
                    # The final ledger also checks every receive through shutdown.
                    changed.wait(0.2)
                    received = ingress[request["topic_request_id"]]
                    result = dict(origin=origin, request_id=request["request_id"],
                                  topic_request_id=request["topic_request_id"],
                                  received_count=received["count"],
                                  input_value=received["request"]["value"],
                                  applied_value=received["request"]["value"] * 7)
                evidence.emit("receipt_executed", request=request, response=result)
                return result

            def add(request):
                validate(request)
                result = dict(origin=origin, request_id=request["request_id"],
                              sum=request["a"] + request["b"])
                evidence.emit("service_executed", request=request, response=result)
                return result

            def cancel(goal):
                evidence.emit("cancel_requested", goal_id=goal.goal_id, request=goal.request)
                return aos.CancelResponse.ACCEPT

            def execute(goal):
                request = goal.request
                validate(request)
                evidence.emit("action_started", goal_id=goal.goal_id, request=request)
                completed, total = 0, 0
                for step in range(1, request["steps"] + 1):
                    if goal.is_cancel_requested:
                        goal.canceled()
                        break
                    if stopping.is_set():
                        goal.abort()
                        break
                    completed, total = step, total + step
                    feedback = dict(origin=origin, request_id=request["request_id"],
                                    completed=completed, total=total)
                    goal.publish_feedback(feedback)
                    evidence.emit("action_feedback", goal_id=goal.goal_id, feedback=feedback)
                    stopping.wait(0.05)
                else:
                    goal.succeed()
                result = dict(origin=origin, request_id=request["request_id"],
                              completed=completed, total=total)
                evidence.emit("action_completed", goal_id=goal.goal_id,
                              status=int(goal.status), result=result)
                return result

            publisher = node.create_publisher(interfaces["topic"], "tick")
            group = aos.ReentrantCallbackGroup()
            node.create_subscription(interfaces["ingress"], "ingress", receive, callback_group=group)
            node.create_service(interfaces["receipt"], "receipt", receipt, callback_group=group)
            node.create_service(interfaces["sum"], "add", add)
            server = aos.ActionServer(
                node, interfaces["action"], "accumulate", execute,
                goal_callback=lambda request: aos.GoalResponse.ACCEPT
                if request["run_id"] == args.run_id
                and request["instance_id"] == origin["instance_id"] else aos.GoalResponse.REJECT,
                cancel_callback=cancel,
            )
            evidence.emit("provider_ready", origin=origin, interfaces={
                key: {"type": value.type_name, "hash": value.type_hash,
                      "definition": value.definition} for key, value in interfaces.items()
            })
            deadline = time.monotonic() + 120
            sequence = 0
            while not args.stop.exists():
                require(time.monotonic() < deadline, "provider lifetime deadline")
                message = dict(origin=origin, sequence=sequence, value=sequence * 3 + 1)
                publisher.publish(message)
                evidence.emit("topic_published", message=message)
                sequence += 1
                stopping.wait(0.05)
            evidence.emit("provider_finished", published=sequence, ingress=ingress)
            del server
        require(compat.installed_binding(build) == build["installed"], "provider install changed")
        evidence.emit("installed_unchanged")
        return 0
    except BaseException as error:
        evidence.emit("worker_failed", error=f"{type(error).__name__}: {error}")
        traceback.print_exc()
        return 1
    finally:
        evidence.stream.close()


def validate_effects(provider, commands, requests, origin):
    """Derive effects from CLI output and provider rows, never from summary flags."""
    message = event(commands["topic-echo"], "message")["data"]["message"]
    require(message["origin"] == origin
            and message["value"] == message["sequence"] * 3 + 1, "Topic echo business value differs")
    one([row for row in provider if row["event"] == "topic_published"
         and row["message"] == message], "provider publication for CLI echo")
    request_ids = set()

    def unique(request):
        require(request["run_id"] == origin["run_id"]
                and request["instance_id"] == origin["instance_id"], "wrong request origin")
        require(request["request_id"] not in request_ids, "business request ID was reused")
        request_ids.add(request["request_id"])

    for mode in ("default", "once"):
        label = "topic-pub-" + mode
        invocation = requests[label]
        require("times" not in invocation and not invocation.get("follow", False)
                and ("once" not in invocation if mode == "default" else invocation.get("once") is True),
                "single-publication test changed its requested mode")
        request = invocation["values"]
        unique(request)
        publication = event(commands[label], "published")["data"]
        require(publication["sequence"] == 1 and publication["submitted"] is True
                and publication["topic_name"] == invocation["topic_name"],
                "single CLI publication count differs")
        actual = one([row for row in provider if row["event"] == "ingress_applied"
                      and row["request"]["request_id"] == request["request_id"]],
                     "provider ingress for single CLI publication")
        require(actual["request"] == request and actual["count"] == 1
                and actual["applied_value"] == request["value"] * 7, "Topic effect differs")
        receipt_request = requests["receipt-" + mode]["values"]
        unique(receipt_request)
        require(receipt_request["topic_request_id"] == request["request_id"], "receipt targets another publish")
        receipt = response(commands["receipt-" + mode])["response"]
        logged = one([row for row in provider if row["event"] == "receipt_executed"
                      and row["request"] == receipt_request], "provider receipt")
        require(logged["response"] == receipt
                and receipt == dict(origin=origin, request_id=receipt_request["request_id"],
                                    topic_request_id=request["request_id"], received_count=1,
                                    input_value=request["value"], applied_value=request["value"] * 7),
                "receipt does not confirm exactly one business effect")
    require(sum(row["event"] == "ingress_applied" for row in provider) == 2,
            "unexpected additional Topic effects")
    require(sum(row["event"] == "receipt_executed" for row in provider) == 2,
            "unexpected additional receipt executions")
    request = requests["service-call"]["values"]
    unique(request)
    actual = event(provider, "service_executed")
    result = response(commands["service-call"])["response"]
    require(actual["request"] == request and actual["response"] == result
            and result == dict(origin=origin, request_id=request["request_id"],
                               sum=request["a"] + request["b"]), "Service business result differs")
    goals = []
    for mode in ("success", "cancel"):
        label = "action-" + mode
        request = requests[label]["goal"]
        unique(request)
        accepted = event(commands[label], "goal_response")["data"]
        goal_id = accepted["goal_id"]
        require(accepted["accepted"] is True and re.fullmatch(r"[0-9a-f]{32}", goal_id),
                "Action goal not accepted or invalid goal ID")
        require(goal_id not in goals, "goal ID reused")
        goals.append(goal_id)
        started = one([row for row in provider if row["event"] == "action_started"
                       and row["goal_id"] == goal_id], "provider Action start")
        completed = one([row for row in provider if row["event"] == "action_completed"
                         and row["goal_id"] == goal_id], "provider Action completion")
        result = event(commands[label], "result")["data"]
        require(started["request"] == request and result["goal_id"] == goal_id
                and completed["result"] == result["result"]
                and completed["status"] == result["status"], "Action ledger/result differs")
        probe.check_result(result, request, origin, canceled=mode == "cancel")
        retained_request = requests[label + "-get-result"]
        retained = response(commands[label + "-get-result"])
        require(retained_request["goal_id"] == goal_id
                and retained == {key: result[key] for key in ("status", "result")},
                "GetResult differs from the observed terminal outcome")
        feedback = [row["data"] for row in commands[label] if row.get("event") == "feedback"]
        require(feedback, "Action feedback was not observed by CLI")
        last_completed = 0
        for item in feedback:
            require(item["goal_id"] == goal_id, "feedback came from another goal")
            value = item["feedback"]
            require(value["origin"] == origin and value["request_id"] == request["request_id"]
                    and last_completed < value["completed"] <= result["result"]["completed"]
                    and value["total"] == value["completed"] * (value["completed"] + 1) // 2,
                    "feedback business progress differs")
            one([row for row in provider if row["event"] == "action_feedback"
                 and row["goal_id"] == goal_id and row["feedback"] == value], "provider feedback")
            last_completed = value["completed"]
        if mode == "cancel":
            require(requests["action-cancel-request"]["goal_id"] == goal_id, "cancel targets another goal")
            canceled = response(commands["action-cancel-request"])
            require(canceled["return_code"] == 0
                    and [info["goal_id"] for info in canceled["goals_canceling"]] == [goal_id],
                    "CancelGoal did not accept this goal")
            cancel = event(provider, "cancel_requested")
            require(cancel["goal_id"] == goal_id and cancel["request"] == request,
                    "provider received a different cancellation")
    require(sum(row["event"] == "action_started" for row in provider) == 2
            and sum(row["event"] == "action_completed" for row in provider) == 2,
            "unexpected Action execution count")
    return {"topic_echo": True, "single_publications": 2, "received_and_applied": 2,
            "service_executions": 1, "action_success": True, "action_canceled": True,
            "feedback_for_both_goals": True, "retained_results": 2,
            "unique_request_ids": sorted(request_ids), "goal_ids": goals}


def validate_case(directory, case, provider_build, cli_build, hashes):
    provider = rows(directory / "provider/provider-1.jsonl")
    ready = event(provider, "provider_ready")
    origin = ready["origin"]
    identity = event(provider, "sdk_ready")
    require(origin["run_id"] == case["run_id"]
            and origin["source_sha"] == provider_build["source_sha"]
            and origin["native_sha256"] == provider_build["installed"]["native_sha256"]
            and origin["probe_sha256"] == hashes["platform_probe.py"], "provider identity differs")
    require(identity["sdk_version"] == provider_build["installed"]["sdk_version"]
            and identity["isolated"] is True
            and identity["worker_sha256"] == hashes[SCRIPT.name]
            and identity["compatibility_helper_sha256"] == hashes["platform_compatibility_probe.py"],
            "provider worker or installed environment differs")
    event(provider, "sdk_closed")
    event(provider, "installed_unchanged")
    require(not any(row["event"] == "worker_failed" for row in provider), "provider failed")
    require(case["provider_returncode"] == 0, "provider did not exit cleanly")
    commands, requests = {}, {}
    for item in case["commands"]:
        require(item["label"] not in commands, "duplicate CLI command label")
        require(item["returncode"] == 0 and not item.get("forced_cleanup"), "CLI did not exit cleanly")
        require(item["cli_sha256"] == cli_build["artifact_sha256"]["cli"], "wrong CLI artifact")
        commands[item["label"]] = rows(directory / item["stdout"])
        if item.get("request"):
            requests[item["label"]] = load(directory / item["request"])
        require(not any(row.get("event") == "error" or row.get("ok") is False
                        for row in commands[item["label"]]), "CLI error was recorded")
    listed = response(commands["node-list"])["nodes"]
    require(any(node["node_id"] == origin["node_id"] for node in listed), "provider not discovered")
    require(response(commands["node-info"])["id"] == origin["node_id"], "wrong NodeCard")
    for group, key, endpoints in (
        ("topic", "topics", {"tick": "topic", "ingress": "ingress"}),
        ("service", "services", {"add": "sum", "receipt": "receipt"}),
        ("action", "actions", {"accumulate": "action"}),
    ):
        listing = response(commands[group + "-list"])[key]
        for name, interface in endpoints.items():
            full = case["namespace"] + "/" + name
            expected = ready["interfaces"][interface]
            entry = one([row for row in listing if row["name"] == full], "discovered endpoint")
            require(entry["types"] == [{"name": expected["type"], "hash": expected["hash"]}],
                    "discovered type/hash differs")
            require(response(commands[name + "-type"]) == {k: expected[k] for k in ("type", "hash")},
                    "resolved type differs")
            require(response(commands[interface + "-interface"]) == expected["definition"],
                    "full remote interface differs")
    for label in ("tick-info", "accumulate-info"):
        require(any(endpoint["node_id"] == origin["node_id"]
                    for endpoint in response(commands[label])["endpoints"]), "info describes another provider")
    require(not any(node["node_id"] == origin["node_id"]
                    for node in response(commands["node-list-after-stop"])["nodes"]),
            "stopped provider remains discoverable")
    effects = validate_effects(provider, commands, requests, origin)
    final = event(provider, "provider_finished")
    require(len(final["ingress"]) == 2
            and all(record["count"] == 1 for record in final["ingress"].values()),
            "provider final ingress count differs")
    return {"status": "passed", "discovery_and_descriptions": True, **effects}


class Runner:
    def __init__(self, args):
        self.args = args
        self.out = args.output.resolve()
        self.out.mkdir(parents=True, exist_ok=False)
        self.copied = self.out / "probe"
        self.copied.mkdir()
        self.cwd = self.out / "unrelated"
        self.cwd.mkdir()
        for source in SOURCE_FILES:
            shutil.copy2(source, self.copied / source.name)
        self.children = []
        self.record = {
            "protocol": PROTOCOL, "status": "running", "started_unix": time.time(),
            "scope": "single-host TCP mixed-version installed CLI method rehearsal",
            "final_C22_acceptance": False,
            "excluded": ["TLS", "cross-machine", "OS service upgrades", "param/lifecycle interoperability"],
            "harness_sha256": {source.name: digest(source) for source in SOURCE_FILES},
            "commands": [], "cases": [],
        }
        repository = SCRIPT.parent.parent
        head = subprocess.run(["git", "-C", str(repository), "rev-parse", "HEAD"],
                              capture_output=True, text=True)
        status = subprocess.run(["git", "-C", str(repository), "status", "--porcelain"],
                                capture_output=True, text=True)
        self.record["harness_source"] = {
            "sha": head.stdout.strip() if head.returncode == 0 else None,
            "status": status.stdout if status.returncode == 0 else None,
            "identity": "script digests are authoritative, including when copied outside Git",
        }
        self.environment = {key: value for key, value in os.environ.items()
                            if not key.startswith(("AOS_", "ZENOH_", "PYTHON"))}

    def start(self, directory, label, command, *, cli_sha256=None, request=None):
        output, error = directory / (label + ".stdout"), directory / (label + ".stderr")
        with output.open("xb") as stdout, error.open("xb") as stderr:
            process = subprocess.Popen(list(map(str, command)), cwd=self.cwd, env=self.environment,
                                       stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr,
                                       start_new_session=True)
        record = dict(label=label, command=list(map(str, command)), pid=process.pid,
                      started_monotonic=time.monotonic(), stdout=output.name, stderr=error.name,
                      cli_sha256=cli_sha256, request=request)
        self.children.append((process, record))
        return process, record

    def finish(self, process, record, timeout=12):
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            record["forced_cleanup"] = True
            probe.stop_process(process, group=True)
        record.update(returncode=process.returncode,
                      seconds=time.monotonic() - record["started_monotonic"])
        require(process.returncode == 0 and not record.get("forced_cleanup"),
                f"{record['label']} failed or timed out; inspect raw stdout/stderr")

    def command(self, directory, label, command, *, bucket=None, **kwargs):
        process, record = self.start(directory, label, command, **kwargs)
        (self.record["commands"] if bucket is None else bucket).append(record)
        self.finish(process, record)
        return rows(directory / record["stdout"])

    def verify_installed(self, label, build, suffix):
        path = self.out / (label + "-build.json")
        save(path, build)
        result = response(self.command(self.out, label + "-installed-" + suffix,
                          [build["python"], "-I", "-B", self.copied / SCRIPT.name,
                           "_installed", "--build", path]))
        require(result == build["installed"], f"{label} installed files or interpreter changed")

    def bind(self):
        previous = load(self.args.installed_evidence)
        require(previous["protocol"] == "aos-platform-compatibility/1"
                and previous["status"] == "passed" and previous["all_children_reaped"],
                "installed-environment evidence did not pass")
        builds = {}
        inputs = self.out / "inputs"
        inputs.mkdir()
        shutil.copy2(self.args.installed_evidence, inputs / "installed-evidence.json")
        for label, version in (("baseline", "0.1.0"), ("candidate", "0.2.0")):
            recorded = previous["builds"][label]
            checked = compat.verified_build(recorded["proof_path"], self.args.router,
                                             baseline=label == "baseline")
            require(all(checked[key] == recorded[key] for key in checked),
                    f"{label} proof or artifact changed since installed verification")
            require(recorded["installed"]["sdk_version"] == version, "unexpected installed version")
            shutil.copy2(recorded["proof_path"], inputs / (label + "-provenance.json"))
            builds[label] = {**checked, "python": recorded["python"], "installed": recorded["installed"]}
            self.verify_installed(label, builds[label], "before")
        require(builds["candidate"]["source_sha"] == self.args.candidate_source_sha,
                "candidate source differs from explicitly requested candidate")
        self.record.update(builds=builds, installed_evidence=str(self.args.installed_evidence.resolve()),
                           installed_evidence_sha256=digest(self.args.installed_evidence))
        return builds

    def introspect(self, label, build):
        help_ = response(self.command(self.out, label + "-help", [build["cli"], "--help"],
                                      cli_sha256=build["artifact_sha256"]["cli"]))
        require(help_["version"] == build["installed"]["sdk_version"], "CLI version differs")
        available = {(entry["group"], entry["verb"]) for entry in help_["commands"]}
        require(set(COMMANDS) <= available, "CLI lacks a required base command")
        schemas = {}
        for group, verb in COMMANDS:
            schema = response(self.command(self.out, f"{label}-schema-{group}-{verb}",
                                          [build["cli"], "schema", group, verb],
                                          cli_sha256=build["artifact_sha256"]["cli"]))
            require(schema["request_schema"]["additionalProperties"] is False,
                    "command has no strict request schema")
            schemas[group, verb] = schema
        return help_, schemas

    def router(self):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        endpoint = f"tcp/127.0.0.1:{port}"
        router = self.out / "router.json"
        save(router, {"mode": "router", "listen": {"endpoints": [endpoint]},
                      "scouting": {"multicast": {"enabled": False},
                                   "gossip": {"enabled": False, "autoconnect": []}}})
        session = self.out / "session.json"
        save(session, {"mode": "client", "connect": {"endpoints": [endpoint],
                       "timeout_ms": 3000, "exit_on_failure": True}, "listen": {"endpoints": []},
                       "scouting": {"multicast": {"enabled": False},
                                    "gossip": {"enabled": False, "autoconnect": []}},
                       "timestamping": {"enabled": True}})
        process, _ = self.start(self.out, "router", [self.args.router, "-c", router])
        deadline = time.monotonic() + 8
        while True:
            require(process.poll() is None, "router exited")
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                    break
            except OSError:
                require(time.monotonic() < deadline, "router listen deadline")
                time.sleep(0.02)
        self.environment.update(AOS_ZENOH_CONFIG=str(session), AOS_DOMAIN_ID="73")
        self.record["network"] = dict(endpoint=endpoint, domain_id=73,
                                       session_sha256=digest(session), router_sha256=digest(self.args.router))
        return session

    def case(self, name, provider, cli, schemas, config):
        directory = self.out / name
        directory.mkdir()
        run_id = "cli_" + uuid.uuid4().hex
        namespace = "/platform_probe/" + run_id
        stop = directory / "stop"
        build_path = directory / "provider-build.json"
        save(build_path, provider)
        case = dict(name=name, run_id=run_id, namespace=namespace, commands=[], status="running",
                    provider_source=provider["source_sha"], cli_source=cli["source_sha"])
        self.record["cases"].append(case)
        process, process_record = self.start(directory, "provider-process", [
            provider["python"], "-I", "-B", "-u", self.copied / SCRIPT.name, "_provider",
            "--build", build_path, "--config", config, "--output", directory / "provider",
            "--run-id", run_id, "--domain-id", "73", "--stop", stop,
        ])
        provider_rows = directory / "provider/provider-1.jsonl"

        def invoke(label, group, verb, request):
            properties = schemas[group, verb]["request_schema"]["properties"]
            request = dict(request)
            if "timeout" in properties and "timeout" not in request:
                request["timeout"] = 8
            require(set(request) <= set(properties), f"{label} uses unsupported fields")
            path = directory / (label + ".request.json")
            save(path, request)
            return self.command(directory, label, [cli["cli"], group, verb, "--request", path],
                                bucket=case["commands"], request=path.name,
                                cli_sha256=cli["artifact_sha256"]["cli"])

        try:
            deadline = time.monotonic() + 8
            while True:
                ready = [row for row in rows(provider_rows, partial=True) if row["event"] == "provider_ready"]
                if ready:
                    origin = one(ready, "provider ready")["origin"]
                    break
                require(process.poll() is None and time.monotonic() < deadline, "provider startup failed")
                time.sleep(0.02)

            def business(**values):
                return dict(run_id=run_id, instance_id=origin["instance_id"],
                            request_id=uuid.uuid4().hex, **values)

            invoke("node-list", "node", "list", {})
            invoke("node-info", "node", "info", {"node_name": namespace + "/cli_provider"})
            for group, endpoints in (("topic", {"tick": "topic", "ingress": "ingress"}),
                                     ("service", {"add": "sum", "receipt": "receipt"}),
                                     ("action", {"accumulate": "action"})):
                invoke(group + "-list", group, "list", {})
                for endpoint, interface in endpoints.items():
                    request = {group + "_name": namespace + "/" + endpoint}
                    invoke(endpoint + "-type", group, "type", request)
                    if endpoint in ("tick", "accumulate"):
                        invoke(endpoint + "-info", group, "info", request)
                    invoke(interface + "-interface", "interface", "show",
                           {"type": DEFINITIONS[interface]["name"]})
            invoke("topic-echo", "topic", "echo", {"topic_name": namespace + "/tick", "once": True})
            for mode in ("default", "once"):
                command = business(value=17 if mode == "default" else -9)
                request = dict(topic_name=namespace + "/ingress", message_type=DEFINITIONS["ingress"]["name"],
                               values=command)
                if mode == "once":
                    request["once"] = True
                if "wait_for_subscribers" in schemas["topic", "pub"]["request_schema"]["properties"]:
                    request.update(wait_for_subscribers=1, linger=0.1)
                invoke("topic-pub-" + mode, "topic", "pub", request)
                invoke("receipt-" + mode, "service", "call", dict(
                    service_name=namespace + "/receipt", service_type=DEFINITIONS["receipt"]["name"],
                    values=business(topic_request_id=command["request_id"])))
            invoke("service-call", "service", "call", dict(service_name=namespace + "/add",
                   service_type=DEFINITIONS["sum"]["name"], values=business(a=29, b=-8)))
            action_base = dict(action_name=namespace + "/accumulate", action_type=DEFINITIONS["action"]["name"])
            success = invoke("action-success", "action", "send_goal",
                             dict(action_base, goal=business(steps=12), feedback=True))
            goal_id = event(success, "goal_response")["data"]["goal_id"]
            invoke("action-success-get-result", "action", "get_result", dict(action_base, goal_id=goal_id))
            request = dict(action_base, goal=business(steps=400), feedback=True, timeout=8)
            path = directory / "action-cancel.request.json"
            save(path, request)
            action_process, action_record = self.start(directory, "action-cancel", [
                cli["cli"], "action", "send_goal", "--request", path],
                cli_sha256=cli["artifact_sha256"]["cli"], request=path.name)
            case["commands"].append(action_record)
            deadline = time.monotonic() + 8
            while True:
                output = rows(directory / action_record["stdout"], partial=True)
                if any(row.get("event") == "feedback" for row in output):
                    goal_id = event(output, "goal_response")["data"]["goal_id"]
                    break
                require(action_process.poll() is None and time.monotonic() < deadline,
                        "cancel test did not reach observed CLI feedback")
                time.sleep(0.01)
            invoke("action-cancel-request", "action", "cancel", dict(action_base, goal_id=goal_id))
            self.finish(action_process, action_record)
            invoke("action-cancel-get-result", "action", "get_result", dict(action_base, goal_id=goal_id))
            stop.touch(exist_ok=False)
            self.finish(process, process_record, timeout=8)
            case["provider_returncode"] = process.returncode
            invoke("node-list-after-stop", "node", "list", {})
            case.update(validate_case(directory, case, provider, cli, self.record["harness_sha256"]))
        except BaseException as error:
            case.update(status="failed", error=f"{type(error).__name__}: {error}")
            traceback.print_exc()
        finally:
            stop.touch(exist_ok=True)
            if process.poll() is None:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    case["forced_provider_cleanup"] = True
            probe.stop_process(process, group=True)
            case["provider_returncode"] = process.returncode
            save(directory / "case.json", case)

    def run(self):
        try:
            require(sys.version_info[:2] == (3, 11) and sys.flags.isolated, "use CPython 3.11 -I")
            builds = self.bind()
            introspection = {label: self.introspect(label, build) for label, build in builds.items()}
            old_help, new_help = (introspection[label][0] for label in ("baseline", "candidate"))
            old_commands = {(c["group"], c["verb"]) for c in old_help["commands"]}
            new_commands = {(c["group"], c["verb"]) for c in new_help["commands"]}
            self.record["semantic_differences"] = {
                "new_cli_commands": sorted(new_commands - old_commands),
                "old_unimplemented": old_help.get("unimplemented", []),
                "topic_pub": {"baseline": "implicitly waits for a subscriber",
                              "candidate": "default does not wait; this probe explicitly requests one subscriber and 0.1s linger",
                              "single_count": "both default count and once=true checked against provider effects"},
                "param_lifecycle": "new command availability is not a mixed-version interoperability result",
            }
            config = self.router()
            for name, provider_label, cli_label in (
                ("old-cli-new-provider", "candidate", "baseline"),
                ("new-cli-old-provider", "baseline", "candidate"),
            ):
                for label in (provider_label, cli_label):
                    compat.verified_build(builds[label]["proof_path"], self.args.router, baseline=label == "baseline")
                self.case(name, builds[provider_label], builds[cli_label], introspection[cli_label][1], config)
            for label, build in builds.items():
                self.verify_installed(label, build, "after")
                compat.verified_build(build["proof_path"], self.args.router, baseline=label == "baseline")
            require(digest(self.args.installed_evidence) == self.record["installed_evidence_sha256"],
                    "installed-environment evidence changed during run")
            require(all(digest(source) == self.record["harness_sha256"][source.name] for source in SOURCE_FILES),
                    "harness changed during run")
            require(all(case["status"] == "passed" for case in self.record["cases"]), "one or more CLI cases failed")
            all_requests = [value for case in self.record["cases"] for value in case["unique_request_ids"]]
            require(len(set(all_requests)) == len(all_requests), "request reused across namespaces")
            self.record["status"] = "passed"
        except BaseException as error:
            self.record.update(status="failed", error=f"{type(error).__name__}: {error}")
            traceback.print_exc()
        finally:
            for process, record in self.children:
                probe.stop_process(process, group=True)
                record["returncode"] = process.returncode
            self.record["all_children_reaped"] = all(p.poll() is not None for p, _ in self.children)
            self.record["children"] = [{"pid": p.pid, "label": r["label"], "returncode": p.returncode}
                                       for p, r in self.children]
            self.record["finished_unix"] = time.time()
            self.record["raw_sha256"] = {str(path.relative_to(self.out)): digest(path)
                                         for path in sorted(self.out.rglob("*"))
                                         if path.is_file() and path.name != "result.json"}
            save(self.out / "result.json", self.record)
            print(json.dumps({"status": self.record["status"], "result": str(self.out / "result.json"),
                              "cases": [{"name": c["name"], "status": c["status"], "error": c.get("error")}
                                        for c in self.record["cases"]]}), flush=True)
        return 0 if self.record["status"] == "passed" else 1


def verify(path):
    result = load(path)
    require(result["protocol"] == PROTOCOL and result["status"] == "passed"
            and result["all_children_reaped"], "run did not pass or was not cleaned up")
    require(result["final_C22_acceptance"] is False, "method rehearsal was relabeled final C22")
    expected_files = {str(target.relative_to(path.parent)) for target in path.parent.rglob("*")
                      if target.is_file() and target.name != "result.json"}
    require(set(result["raw_sha256"]) == expected_files, "raw evidence inventory differs")
    for relative, expected in result["raw_sha256"].items():
        target = path.parent / relative
        require(target.resolve().is_relative_to(path.parent.resolve()), "raw path escapes evidence")
        require(digest(target) == expected, f"raw evidence changed: {relative}")
    for name, expected in result["harness_sha256"].items():
        require(digest(path.parent / "probe" / name) == expected, "copied harness differs")
    require(digest(path.parent / "inputs/installed-evidence.json") == result["installed_evidence_sha256"],
            "copied installed-environment evidence differs")
    for label, build in result["builds"].items():
        require(digest(path.parent / "inputs" / (label + "-provenance.json")) == build["proof_sha256"],
                "copied build provenance differs")
        help_ = response(rows(path.parent / (label + "-help.stdout")))
        require(help_["version"] == build["installed"]["sdk_version"], "recorded CLI version differs")
        require(set(COMMANDS) <= {(c["group"], c["verb"]) for c in help_["commands"]},
                "required commands absent from recorded help")
        for group, verb in COMMANDS:
            schema = response(rows(path.parent / f"{label}-schema-{group}-{verb}.stdout"))
            require(schema["request_schema"]["additionalProperties"] is False,
                    "recorded command schema is not strict")
    require([case["name"] for case in result["cases"]] == ["old-cli-new-provider", "new-cli-old-provider"],
            "missing mixed-version direction")
    verified = []
    for case in result["cases"]:
        provider_label, cli_label = (("candidate", "baseline") if case["name"] == "old-cli-new-provider"
                                     else ("baseline", "candidate"))
        verified.append(validate_case(path.parent / case["name"], case, result["builds"][provider_label],
                                      result["builds"][cli_label], result["harness_sha256"]))
    ids = [request for case in verified for request in case["unique_request_ids"]]
    require(len(ids) == len(set(ids)), "request IDs reused across directions")
    return {"status": "passed", "verified_directions": len(verified), "effects": verified,
            "result_sha256": digest(path), "scope": result["scope"], "final_C22_acceptance": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest="mode", required=True)
    run = modes.add_parser("run")
    for name in ("installed-evidence", "router", "output"):
        run.add_argument("--" + name, type=Path, required=True)
    run.add_argument("--candidate-source-sha", required=True)
    audit = modes.add_parser("verify")
    audit.add_argument("--result", type=Path, required=True)
    worker = modes.add_parser("_provider")
    for name in ("build", "config", "output", "stop"):
        worker.add_argument("--" + name, type=Path, required=True)
    worker.add_argument("--run-id", required=True)
    worker.add_argument("--domain-id", type=int, required=True)
    installed = modes.add_parser("_installed")
    installed.add_argument("--build", type=Path, required=True)
    args = parser.parse_args()
    if args.mode == "_provider":
        return provider_worker(args)
    if args.mode == "_installed":
        print(json.dumps({"ok": True, "data": compat.installed_binding(load(args.build))}))
        return 0
    if args.mode == "verify":
        print(json.dumps(verify(args.result.resolve()), sort_keys=True))
        return 0
    return Runner(args).run()


if __name__ == "__main__":
    raise SystemExit(main())
