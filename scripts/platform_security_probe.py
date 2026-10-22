#!/usr/bin/env python3
"""C18 secure recovery probe, run with an installed-wheel Python and -I.

This reuses platform_probe's interfaces, result checks, graph wait, and process
cleanup. Fault epochs, owned SIGKILL/reap/port checks, and fresh business IDs
follow the Stage 8 soak probe. It never builds, installs, uploads, uses SSH,
changes a firewall, or signals an existing external process.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import contextmanager
import hashlib
import importlib.machinery
import importlib.util
import json
import os
from pathlib import Path
import platform
import re
import signal
import socket
import subprocess
import sys
import sysconfig
import tempfile
import threading
import time
import traceback
import uuid
import zipfile

SCRIPT = Path(__file__).resolve()
BASE_SCRIPT = SCRIPT.with_name("platform_probe.py")
SPEC = importlib.util.spec_from_file_location("aos_base_probe", BASE_SCRIPT)
base = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(base)
require, digest, Deadline = base.require, base.digest, base.Deadline
DIRECTIONS = ("a_to_b", "b_to_a")
PROTOCOL = "aos.secure.recovery.v1"


def read_json(path):
    return json.loads(Path(path).read_text())


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")
    temporary.replace(path)


def environment():
    return {key: value for key, value in os.environ.items()
            if not key.startswith("AOS_") and key not in ("PYTHONPATH", "PYTHONHOME")}


def namespace(args, direction=None):
    return f"/platform_probe/{args.run_id}_{direction or args.direction}"


def policy(args):
    subjects = {name: dict(id=name, common_name=name, nodes=[], endpoints=[],
                           systems=[], graph_nodes=[]) for name in ("host_a", "host_b")}
    for direction in DIRECTIONS:
        ns = namespace(args, direction)
        provider, client = ({"name": name, "namespace": ns} for name in ("provider", "client"))
        names = ("host_a", "host_b") if direction == "a_to_b" else ("host_b", "host_a")
        for subject_name, node, is_provider in ((names[0], provider, True), (names[1], client, False)):
            subject = subjects[subject_name]
            subject["nodes"].append(node)
            if not is_provider:
                subject["graph_nodes"].append(provider)
                subject["systems"].append(dict(node=node, target=provider, operations=["describe"]))
            for kind, endpoint, operations in (
                ("topic", "tick", ["publish"] if is_provider else ["subscribe"]),
                ("sum", "add", ["provide"] if is_provider else ["call"]),
                ("control", "finish_round", ["provide"] if is_provider else ["call"]),
                ("action", "accumulate", ["provide"] if is_provider else
                 ["send_goal", "get_result", "cancel_goal", "feedback", "status"]),
            ):
                subject["endpoints"].append(dict(node=node, namespace=ns, name=ns + "/" + endpoint,
                    interface=base.DEFINITIONS[kind], operations=operations))
    return dict(schema_version=1, domain_id=args.domain_id, subjects=list(subjects.values()))


def binding(path):
    """Verify actual installed bytes; source provenance remains the supplied build record."""
    import aos_sdk as aos
    from aos_sdk import _native
    require(sys.flags.isolated, "use Python -I")
    proof = read_json(path)
    require(re.fullmatch(r"[0-9a-f]{40}", proof["source_sha"]), "full source SHA required")
    require(isinstance(_native.__loader__, importlib.machinery.ExtensionFileLoader), "native module is not an extension")
    native = Path(_native.__file__).resolve()
    site = Path(sysconfig.get_paths()["platlib"]).resolve()
    require(native.parent == site / "aos_sdk" and Path(aos.__file__).resolve().parent == native.parent,
            "SDK is not installed in this interpreter")
    for name, file in (("cli", Path(proof["cli_path"])), ("wheel", Path(proof["wheel_path"])), ("native", native)):
        require(digest(file) == proof[name + "_sha256"], name + " artifact differs from proof")
    members = proof["wheel_installed_members"]
    require(members and len({item["member"] for item in members}) == len(members), "invalid installed member proof")
    with zipfile.ZipFile(proof["wheel_path"]) as wheel:
        expected = {name for name in wheel.namelist() if name.startswith("aos_sdk/") and not name.endswith("/")}
        require(expected == {item["member"] for item in members}, "proof omits a wheel package member")
        for item in members:
            raw = wheel.read(item["member"])
            require(hashlib.sha256(raw).hexdigest() == item["sha256"] == digest(site / item["member"]),
                    "installed package member differs: " + item["member"])
    return dict(source_sha=proof["source_sha"], cli_path=proof["cli_path"],
                cli_sha256=proof["cli_sha256"], wheel_sha256=proof["wheel_sha256"],
                native_sha256=digest(native), verified_members=len(members), proof_sha256=digest(path),
                probe_sha256=digest(SCRIPT), base_probe_sha256=digest(BASE_SCRIPT),
                source_binding="caller supplied build record; executable/package bytes verified")


class Evidence:
    def __init__(self, args, name):
        self.args, self.name = args, name
        self.lock = threading.Lock()
        self.path = args.output / (name + ".jsonl")
        self.stream = self.path.open("x")
        self.binding = binding(args.binding)
        self.metadata = dict(protocol=PROTOCOL, run_id=args.run_id, direction=args.direction,
            role=name, pid=os.getpid(), hostname=socket.gethostname(), platform=platform.platform(),
            machine=platform.machine(), isolated=bool(sys.flags.isolated), binding=self.binding,
            expected_peer_source_sha=getattr(args, "peer_source_sha", None) or self.binding["source_sha"])

    def emit(self, event, **data):
        with self.lock:
            item = dict(self.metadata, event=event, unix_ns=time.time_ns(), monotonic=time.monotonic(), **data)
            line = json.dumps(item, sort_keys=True, allow_nan=False)
            self.stream.write(line + "\n")
            self.stream.flush()
            print(line, flush=True)
            return item

    def close(self):
        require(binding(self.args.binding) == self.binding, "artifacts or scripts changed during run")
        self.emit("finished")
        self.stream.close()


@contextmanager
def runtime(args, evidence, role):
    import aos_sdk as aos
    profile = read_json(args.profile)
    require(profile["domain_id"] == args.domain_id, "profile domain differs from probe")
    context = aos.Context(security_profile=args.profile)
    node = aos.Node(role, namespace(args), context=context)
    executor = aos.MultiThreadedExecutor(4, context=context)
    executor.add_node(node)
    stop = threading.Event()
    spinner = threading.Thread(target=executor.spin, daemon=True)
    spinner.start()
    evidence.emit("sdk_ready", node_id=node.get_node_card()["id"], profile_sha256=digest(args.profile),
                  endpoint=profile["router_endpoints"], domain=args.domain_id)
    try:
        yield aos, context, node, stop
    finally:
        stop.set()
        context.shutdown()
        clean = executor.shutdown(timeout_sec=3)
        spinner.join(1)
        node.destroy_node()
        require(clean and not spinner.is_alive() and not context.ok(), "SDK cleanup failed")
        evidence.emit("sdk_closed")


def provider_worker(args, evidence):
    with runtime(args, evidence, "provider") as (aos, _, node, stop):
        types = {key: aos.Interface.load(value) for key, value in base.DEFINITIONS.items()}
        origin = dict(run_id=args.run_id, instance_id=uuid.uuid4().hex, round=args.generation,
            pid=os.getpid(), hostname=socket.gethostname(), node_id=node.get_node_card()["id"],
            source_sha=evidence.binding["source_sha"], native_sha256=evidence.binding["native_sha256"],
            probe_sha256=digest(SCRIPT))
        final = {}

        def validate(request):
            valid = request["run_id"] == args.run_id and request["instance_id"] == origin["instance_id"]
            if not valid:
                evidence.emit("stale_request_observed", request=request, origin=origin)
            require(valid, "stale business request")

        def add(request):
            evidence.emit("service_received", request=request, origin=origin)
            validate(request)
            evidence.emit("service_executed", request=request, origin=origin)
            return dict(origin=origin, request_id=request["request_id"], sum=request["a"] + request["b"])

        def control(request):
            validate(request)
            expected = "restart" if args.generation == 1 else "stop"
            require(request["operation"] == expected and not final, "unexpected provider control")
            final.update(operation=expected, at=time.monotonic() + .3)
            evidence.emit("control_executed", request=request, origin=origin)
            return dict(origin=origin, request_id=request["request_id"], operation=expected)

        def execute(goal):
            request = goal.request
            validate(request)
            evidence.emit("action_started", request=request, origin=origin, goal_id=goal.goal_id)
            count, total = 0, 0
            for count_next in range(1, request["steps"] + 1):
                if goal.is_cancel_requested:
                    goal.canceled()
                    break
                if stop.is_set():
                    goal.abort()
                    break
                count, total = count_next, total + count_next
                goal.publish_feedback(dict(origin=origin, request_id=request["request_id"], completed=count, total=total))
                stop.wait(.025)
            else:
                goal.succeed()
            result = dict(origin=origin, request_id=request["request_id"], completed=count, total=total)
            evidence.emit("action_completed", result=result, status=int(goal.status), goal_id=goal.goal_id)
            return result

        def accept_goal(request):
            evidence.emit("action_goal_received", request=request, origin=origin)
            validate(request)
            return aos.GoalResponse.ACCEPT

        pub = node.create_publisher(types["topic"], "tick")
        node.create_service(types["sum"], "add", add)
        node.create_service(types["control"], "finish_round", control)
        aos.ActionServer(node, types["action"], "accumulate", execute,
                         goal_callback=accept_goal, cancel_callback=lambda _: aos.CancelResponse.ACCEPT)
        evidence.emit("provider_ready", origin=origin)
        deadline, sequence = Deadline(args.timeout), 0
        while True:
            deadline.remaining()
            if final and time.monotonic() >= final["at"]:
                write_json(args.output / f"provider-control-{args.generation}.json", final)
                if final["operation"] == "stop":
                    return
                # Supervisor owns the actual SIGKILL; no graceful terminal is fabricated.
                while True:
                    deadline.remaining()
                    time.sleep(.05)
            pub.publish(dict(origin=origin, sequence=sequence, value=sequence * 3 + args.generation))
            sequence += 1
            stop.wait(.05)


def child_command(args, mode, output, profile=None, generation=0):
    command = [sys.executable, "-I", "-u", str(SCRIPT), mode, "--binding", str(args.binding),
               "--output", str(output), "--run-id", args.run_id, "--direction", args.direction,
               "--domain-id", str(args.domain_id), "--timeout", str(args.timeout),
               "--stage-timeout", str(args.stage_timeout), "--generation", str(generation)]
    if getattr(args, "peer_source_sha", None):
        command += ["--peer-source-sha", args.peer_source_sha]
    if profile or getattr(args, "profile", None):
        command += ["--profile", str(profile or args.profile)]
    return command


def start(command, log, *, group=False):
    with Path(log).open("xb") as stream:
        return subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL, env=environment(), start_new_session=group)


def wait_until(predicate, seconds, message, check=lambda: None):
    deadline = Deadline(seconds)
    while True:
        check()
        value = predicate()
        if value:
            return value
        try:
            deadline.remaining()
        except TimeoutError as error:
            raise TimeoutError(message) from error
        time.sleep(.03)


def provider(args, evidence):
    for generation in (1, 2):
        process = start(child_command(args, "_provider", args.output, generation=generation),
                        args.output / f"provider-{generation}.stdout")
        evidence.emit("provider_started", generation=generation, child_pid=process.pid)
        try:
            path = args.output / f"provider-control-{generation}.json"
            def check():
                require(process.poll() is None or (generation == 2 and process.returncode == 0 and path.exists()),
                        "provider exited before control")
            wait_until(path.exists, args.timeout, "provider control timeout", check)
            if generation == 1:
                process.kill()
                process.wait(5)
                require(process.returncode == -signal.SIGKILL, "provider fault was not SIGKILL")
                evidence.emit("provider_killed", generation=generation, child_pid=process.pid, reaped=True)
                time.sleep(2)
            else:
                require(process.wait(5) == 0, "provider final close failed")
        finally:
            base.stop_process(process)
            evidence.emit("provider_reaped", generation=generation, child_pid=process.pid, returncode=process.returncode)


def client(args, evidence):
    with runtime(args, evidence, "client") as (aos, context, node, _):
        types = {key: aos.Interface.load(value) for key, value in base.DEFINITIONS.items()}
        sub = node.create_subscription(types["topic"], "tick")
        service = node.create_client(types["sum"], "add")
        control = node.create_client(types["control"], "finish_round")
        action = aos.ActionClient(node, types["action"], "accumulate")
        last_sequence = {}

        def request(origin, phase, **fields):
            return dict(run_id=args.run_id, instance_id=origin["instance_id"],
                        request_id=phase + "_" + uuid.uuid4().hex, **fields)

        def graph_has(origin, present):
            return base.wait_graph(context, lambda entries: any(
                item["node_id"] == origin["node_id"] for item in entries) == present, args.stage_timeout)

        def next_tick(generation, previous=None):
            deadline = Deadline(args.stage_timeout)
            while True:
                tick = sub.recv(deadline.remaining())
                origin = tick["origin"]
                if origin["round"] != generation:
                    continue
                require(origin["run_id"] == args.run_id and origin["source_sha"] == evidence.metadata["expected_peer_source_sha"]
                        and origin["probe_sha256"] == digest(SCRIPT), "provider source or run identity differs")
                if previous:
                    require(origin == previous, "router recovery changed provider instance")
                require(tick["value"] == tick["sequence"] * 3 + generation, "invalid Topic arithmetic")
                old = last_sequence.get(origin["instance_id"], -1)
                require(tick["sequence"] > old, "Topic replay or reorder")
                last_sequence[origin["instance_id"]] = tick["sequence"]
                return tick

        def begin_goal(origin, phase, steps):
            req = request(origin, phase, steps=steps)
            event, feedbacks = threading.Event(), []
            def feedback(value):
                feedbacks.append(value)
                event.set()
            evidence.emit("action_requested", phase=phase, request=req)
            goal = action.send_goal_async(req, feedback_callback=feedback, timeout=args.stage_timeout).result(args.stage_timeout + 1)
            require(goal.accepted and event.wait(args.stage_timeout), "Action was rejected or produced no feedback")
            first = feedbacks[0]
            require(first["goal_id"] == goal.goal_id and first["feedback"]["origin"] == origin
                    and first["feedback"]["request_id"] == req["request_id"], "feedback identity differs")
            return req, goal, goal.get_result_async(timeout=args.stage_timeout), first

        def phase(name, generation, previous=None):
            require(service.wait_for_service(args.stage_timeout) and action.wait_for_server(args.stage_timeout),
                    "recovery did not restore Service/Action readiness")
            tick = next_tick(generation, previous)
            origin = tick["origin"]
            graph = graph_has(origin, True)
            evidence.emit("topic_verified", phase=name, message=tick, graph=graph)
            req = request(origin, name, a=19 + generation, b=23)
            evidence.emit("service_requested", phase=name, request=req)
            response = service.call(req, timeout=args.stage_timeout)
            require(response == dict(origin=origin, request_id=req["request_id"], sum=req["a"] + req["b"]), "wrong Service result")
            evidence.emit("service_verified", phase=name, request=req, response=response)
            for canceled in (False, True):
                req, goal, result, first = begin_goal(origin, name, 400 if canceled else 6)
                if canceled:
                    response = goal.cancel_goal_async(timeout=args.stage_timeout).result(args.stage_timeout + 1)
                    require(goal.goal_id in [item["goal_id"] for item in response["goals_canceling"]], "cancel omitted goal")
                    evidence.emit("cancel_accepted", phase=name, goal_id=goal.goal_id, response=response)
                response = result.result(args.stage_timeout + 1)
                base.check_result(response, req, origin, canceled=canceled)
                evidence.emit("action_cancel_verified" if canceled else "action_success_verified",
                              phase=name, request=req, response=response, first_feedback=first)
            evidence.emit("phase_verified", phase=name, origin=origin)
            return origin

        original = phase("initial", 1)
        evidence.emit("router_fault_ready", origin=original)
        graph = graph_has(original, False)
        evidence.emit("router_down_observed", origin=original, graph=graph)
        graph_has(original, True)
        # Discard pre-fault queued samples before accepting a post-fault tick.
        while True:
            try:
                sub.recv(.01)
            except aos.WaitTimeoutError:
                break
        phase("router_recovered", 1, original)
        req, goal, pending, _ = begin_goal(original, "provider_fault_pending", 400)
        command = request(original, "provider_restart", operation="restart")
        response = control.call(command, timeout=args.stage_timeout)
        require(response == dict(origin=original, request_id=command["request_id"], operation="restart"), "restart control failed")
        graph = graph_has(original, False)
        evidence.emit("old_provider_gone", origin=original, graph=graph)
        try:
            response = pending.result(args.stage_timeout + 1)
            require(response["status"] == int(aos.GoalStatus.UNKNOWN), "lost provider fabricated a terminal result")
            evidence.emit("interrupted_action_unresolved", request=req, response=response)
        except (aos.WaitTimeoutError, aos.NoResponseError, aos.CommunicationError, TimeoutError) as error:
            evidence.emit("interrupted_action_unresolved", request=req, error=type(error).__name__)
        recovered = phase("provider_recovered", 2)
        require(all(recovered[key] != original[key] for key in ("instance_id", "node_id", "pid")), "provider did not restart")
        command = request(recovered, "final_stop", operation="stop")
        response = control.call(command, timeout=args.stage_timeout)
        require(response == dict(origin=recovered, request_id=command["request_id"], operation="stop"), "final control failed")
        graph = graph_has(recovered, False)
        evidence.emit("final_provider_gone", origin=recovered, graph=graph)


def port_open(host, port):
    try:
        with socket.create_connection((host, port), timeout=.2):
            return True
    except OSError:
        return False


class Router:
    def __init__(self, args, evidence):
        self.args, self.evidence, self.process, self.epoch = args, evidence, None, 0
        request = read_json(args.request)
        require(request["policy"] == policy(args), "router policy does not match this exact probe")
        endpoint = request["router"]["listen_endpoints"]
        require(len(endpoint) == 1 and endpoint[0].startswith("tls/"), "one native TLS listener required")
        self.host, raw_port = endpoint[0][4:].rsplit(":", 1)
        self.host = "127.0.0.1" if self.host == "0.0.0.0" else self.host
        self.port = int(raw_port)

    def up(self):
        require(self.process is None and not port_open(self.host, self.port), "router port already in use")
        self.epoch += 1
        log = self.args.output / f"router-{self.epoch}.log"
        self.process = start([self.evidence.binding["cli_path"], "security", "serve", "--request", str(self.args.request)], log)
        def ready():
            require(self.process.poll() is None, "router exited before ready")
            for line in log.read_text().splitlines():
                try:
                    item = json.loads(line)
                except ValueError:
                    continue
                if item.get("event") == "router_ready":
                    return item["data"]
        value = wait_until(ready, self.args.stage_timeout, "router startup timeout")
        require(value["locators"] and all(item.startswith("tls/") for item in value["locators"]), "non-TLS listener")
        self.evidence.emit("router_started", epoch=self.epoch, child_pid=self.process.pid, ready=value)

    def down(self, crash):
        if self.process is None:
            return
        process = self.process
        if crash:
            process.kill()
            process.wait(5)
            require(process.returncode == -signal.SIGKILL, "router did not exit by SIGKILL")
        else:
            base.stop_process(process)
            summaries = [item for item in events(self.args.output / f"router-{self.epoch}.log")
                         if item.get("event") == "summary"]
            require(process.returncode == 143 and summaries and summaries[-1]["data"]["session_closed"]
                    and summaries[-1]["data"]["cleanup_error"] is None, "router did not confirm graceful cleanup")
        require(not port_open(self.host, self.port), "owned router port remained open")
        self.evidence.emit("router_stopped", epoch=self.epoch, child_pid=process.pid,
                           crash=crash, returncode=process.returncode, reaped=True, port_closed=True)
        self.process = None


def router(args, evidence):
    managed = Router(args, evidence)
    last = None
    try:
        managed.up()
        deadline = Deadline(args.timeout)
        while True:
            deadline.remaining()
            if args.control_file.exists():
                command = read_json(args.control_file)
                if command["nonce"] != last:
                    last = command["nonce"]
                    if command["operation"] == "down":
                        managed.down(True)
                    elif command["operation"] == "up":
                        managed.up()
                    elif command["operation"] == "stop":
                        break
                    else:
                        raise ValueError("unknown local router control")
            if managed.process:
                require(managed.process.poll() is None, "router exited unexpectedly")
            time.sleep(.05)
    finally:
        managed.down(False)


def events(path, *, strict=False):
    if not Path(path).exists():
        return []
    raw = Path(path).read_text()
    if strict:
        require(raw.endswith("\n"), "incomplete evidence file: " + str(path))
    # A concurrently written final line is not yet evidence. Earlier malformed
    # lines fail instead of being silently discarded.
    lines = raw.splitlines() if raw.endswith("\n") else raw.splitlines()[:-1]
    result = []
    for line in lines:
        if strict or line.startswith("{"):
            result.append(json.loads(line))
    return result


def verify(client_path, provider_path, router_path):
    c = events(Path(client_path) / "client.jsonl", strict=True)
    p = sum((events(Path(provider_path) / f"_provider-{generation}.jsonl", strict=True) for generation in (1, 2)), [])
    supervisor = events(Path(provider_path) / "provider.jsonl", strict=True)
    r = events(Path(router_path) / "router.jsonl", strict=True)
    checks = {}
    def check(name, value):
        checks[name] = bool(value)
    required = ("initial", "router_recovered", "provider_recovered")
    for phase_name in required:
        for event in ("phase_verified", "topic_verified", "service_verified", "action_success_verified", "action_cancel_verified"):
            check(phase_name + ":" + event, sum(item["event"] == event and item.get("phase") == phase_name for item in c) == 1)
    for key, wanted, actual, expected_count in (("service", "service_requested", "service_executed", 3),
                                               ("action", "action_requested", "action_started", 7)):
        issued = [item["request"]["request_id"] for item in c if item["event"] == wanted]
        executed = [item["request"]["request_id"] for item in p if item["event"] == actual]
        check(key + "_issued_once", len(issued) == len(set(issued)) == expected_count)
        check(key + "_executed_exactly_once", Counter(issued) == Counter(executed) and all(n == 1 for n in Counter(executed).values()))
        check(key + "_executed_payloads_match", Counter(json.dumps(item["request"], sort_keys=True) for item in c if item["event"] == wanted)
              == Counter(json.dumps(item["request"], sort_keys=True) for item in p if item["event"] == actual))
        received_event = "service_received" if key == "service" else "action_goal_received"
        check(key + "_provider_received_once", Counter(issued) == Counter(item["request"]["request_id"] for item in p
                                                                         if item["event"] == received_event))
    try:
        phase_origins = {item["phase"]: item["origin"] for item in c if item["event"] == "phase_verified"}
        require(phase_origins["initial"] == phase_origins["router_recovered"], "provider changed across router fault")
        require(all(phase_origins["initial"][key] != phase_origins["provider_recovered"][key]
                    for key in ("instance_id", "node_id", "pid")), "provider identity did not change")
        for item in c:
            if item["event"] == "topic_verified":
                tick = item["message"]
                require(tick["origin"] == phase_origins[item["phase"]] and
                        tick["value"] == tick["sequence"] * 3 + tick["origin"]["round"], "bad Topic payload")
            if item["event"] == "service_verified":
                req = item["request"]
                require(item["response"] == dict(origin=phase_origins[item["phase"]], request_id=req["request_id"],
                                                  sum=req["a"] + req["b"]), "bad Service payload")
            if item["event"] in ("action_success_verified", "action_cancel_verified"):
                base.check_result(item["response"], item["request"], phase_origins[item["phase"]],
                                  canceled=item["event"] == "action_cancel_verified")
                match = [entry for entry in p if entry["event"] == "action_completed"
                         and entry["result"]["request_id"] == item["request"]["request_id"]]
                require(len(match) == 1 and {key: match[0][key] for key in ("status", "result")} == item["response"],
                        "Action terminal disagrees with provider")
        check("business_payloads_and_provider_identities", True)
    except (KeyError, ValueError, TypeError, RuntimeError):
        check("business_payloads_and_provider_identities", False)
    pending = [item["request"]["request_id"] for item in c if item["event"] == "interrupted_action_unresolved"]
    completed = [item["result"]["request_id"] for item in p if item["event"] == "action_completed"]
    check("interrupted_goal_no_fabricated_terminal", len(pending) == 1 and pending[0] not in completed)
    unresolved = [item for item in c if item["event"] == "interrupted_action_unresolved"]
    check("interrupted_goal_is_unknown_or_transport_error", len(unresolved) == 1 and
          (unresolved[0].get("response", {}).get("status") == 0 or unresolved[0].get("error") in
           ("WaitTimeoutError", "NoResponseError", "CommunicationError", "TimeoutError")))
    for event in ("router_fault_ready", "router_down_observed", "old_provider_gone", "final_provider_gone", "sdk_closed", "finished"):
        check("client:" + event, sum(item["event"] == event for item in c) == 1)
    check("provider_crash_owned_and_reaped", sum(item["event"] == "provider_killed" and item.get("reaped") for item in supervisor) == 1)
    check("provider_final_close", any(item["event"] == "sdk_closed" for item in p) and any(item["event"] == "finished" for item in supervisor))
    check("retained_client_context", sum(item["event"] == "sdk_ready" for item in c) == 1
          and len({item["pid"] for item in c}) == 1)
    check("no_failed_events", not any(item["event"] == "failed" for item in c + p + supervisor + r))
    check("no_stale_request_attempts", not any(item["event"] == "stale_request_observed" for item in p))
    starts = [item for item in r if item["event"] == "router_started"]
    check("router_reauthenticated_topology", len(starts) == 2 and starts[0]["child_pid"] != starts[1]["child_pid"]
          and starts[0]["ready"]["zid"] != starts[1]["ready"]["zid"]
          and starts[0]["ready"]["policy_digest"] == starts[1]["ready"]["policy_digest"]
          and all(item["ready"]["locators"] and all(endpoint.startswith("tls/") for endpoint in item["ready"]["locators"])
                  for item in starts))
    stops = [item for item in r if item["event"] == "router_stopped"]
    check("router_crash_and_cleanup", len(stops) == 2 and stops[0]["crash"] and not stops[1]["crash"]
          and all(item["reaped"] and item["port_closed"] for item in stops))
    check("same_run_and_probe", bool(c and p and supervisor and r) and len({(item["run_id"], item["direction"],
          item["binding"]["probe_sha256"], item["binding"]["base_probe_sha256"]) for item in c + p + supervisor + r}) == 1)
    source_pairs, identities = {}, {}
    for role, rows in (("client", c), ("provider", p + supervisor), ("router", r)):
        pairs = {(item["binding"]["source_sha"],
                  item.get("expected_peer_source_sha", item["binding"]["source_sha"])) for item in rows}
        source_pairs[role] = next(iter(pairs)) if len(pairs) == 1 else None
        identities[role] = {(item["hostname"], json.dumps(item["binding"], sort_keys=True)) for item in rows}
    check("stable_source_bindings", all(pair is not None and all(re.fullmatch(r"[0-9a-f]{40}", sha) for sha in pair)
          and len(identities[role]) == 1 for role, pair in source_pairs.items()))
    check("declared_peer_sources_match", source_pairs["client"] is not None and source_pairs["provider"] is not None
          and source_pairs["client"] == source_pairs["provider"][::-1])
    router_source_roles = [role for role in ("client", "provider") if len(identities["router"]) == 1
                           and identities["router"] == identities[role]
                           and source_pairs["router"] == source_pairs[role]]
    check("router_source_matches_endpoint", bool(router_source_roles))
    hosts = sorted({item["hostname"] for item in c + p})
    return dict(protocol=PROTOCOL, status="passed" if all(checks.values()) else "failed", checks=checks,
                hostnames=hosts, cross_machine_observed=len(hosts) > 1,
                source_bindings={role: dict(source_sha=pair[0], expected_peer_source_sha=pair[1]) if pair else None
                                 for role, pair in source_pairs.items()}, router_source_roles=router_source_roles,
                scope="one direction; hostname difference alone does not establish Mac/Linux or network-path acceptance")


def local(args):
    import aos_sdk as aos
    checked = binding(args.binding)
    results = {}
    with tempfile.TemporaryDirectory(prefix="aos-c18-pki-") as temporary:
        pki = Path(temporary)
        pki.chmod(0o700)
        def openssl(*command):
            return subprocess.run(["openssl", *command], cwd=pki, capture_output=True, check=True, timeout=15).stdout.decode()
        openssl("req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes", "-sha256",
                "-days", "2", "-subj", "/CN=AOS C18 local test CA", "-keyout", "ca.key", "-out", "ca.pem",
                "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign")
        for serial, name in enumerate(("server", "host_a", "host_b"), 1):
            openssl("req", "-new", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes", "-subj",
                    "/CN=" + name, "-keyout", name + ".key", "-out", name + ".csr")
            (pki / "leaf.ext").write_text("basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyAgreement\n"
                "extendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=IP:127.0.0.1,DNS:localhost\n")
            openssl("x509", "-req", "-sha256", "-days", "2", "-in", name + ".csr", "-CA", "ca.pem", "-CAkey", "ca.key",
                    "-set_serial", str(serial), "-extfile", "leaf.ext", "-out", name + ".pem")
        write_json(args.output / "public-certificate-metadata.json", {name: openssl("x509", "-in", name + ".pem", "-noout",
                   "-subject", "-issuer", "-serial", "-dates", "-sha256", "-fingerprint").splitlines()
                   for name in ("ca", "server", "host_a", "host_b")})
        def files(name):
            return dict(schema_version=1, root_ca_certificate=str(pki / "ca.pem"),
                        certificate=str(pki / (name + ".pem")), private_key=str(pki / (name + ".key")))
        write_json(args.output / "policy.json", policy(args))
        compiled = aos.compile_security_policy(policy(args))
        write_json(args.output / "compiled-policy.json", compiled)
        for direction in DIRECTIONS:
            current = argparse.Namespace(**vars(args))
            current.direction, current.output = direction, args.output / direction
            current.output.mkdir()
            with socket.socket() as reserved:
                reserved.bind(("127.0.0.1", 0))
                port = reserved.getsockname()[1]
            endpoint = "tls/127.0.0.1:" + str(port)
            profiles = {}
            for name in ("host_a", "host_b"):
                profile = pki / (name + ".json")
                write_json(profile, files(name) | dict(domain_id=args.domain_id, router_endpoints=[endpoint], connect_timeout_ms=3000))
                write_json(current.output / (name + "-profile.json"), read_json(profile))
                profiles[name] = profile
            current.request = pki / "router-request.json"
            write_json(current.request, dict(router=files("server") | dict(listen_endpoints=[endpoint]),
                       policy=policy(args), startup_timeout=5, shutdown_timeout=2))
            write_json(current.output / "router-request.json", read_json(current.request))
            evidence = Evidence(current, "router")
            managed, children = Router(current, evidence), []
            try:
                managed.up()
                names = ("host_a", "host_b") if direction == "a_to_b" else ("host_b", "host_a")
                for role, name in zip(("provider", "client"), names):
                    output = current.output / role
                    child = start(child_command(current, role, output, profiles[name]), current.output / (role + ".stdout"), group=True)
                    children.append(child)
                def check():
                    require(all(child.poll() in (None, 0) for child in children), "probe child failed; inspect its JSONL/stdout")
                client_log = current.output / "client/client.jsonl"
                wait_until(lambda: any(item["event"] == "router_fault_ready" for item in events(client_log)), args.stage_timeout * 3, "client startup", check)
                managed.down(True)
                wait_until(lambda: any(item["event"] == "router_down_observed" for item in events(client_log)), args.stage_timeout, "client did not observe router loss", check)
                managed.up()
                wait_until(lambda: all(child.poll() == 0 for child in children), args.timeout, "children did not finish", check)
            finally:
                for child in children:
                    base.stop_process(child, group=True)
                managed.down(False)
                evidence.close()
            result = verify(current.output / "client", current.output / "provider", current.output)
            results[direction] = result
            write_json(current.output / "result.json", result)
            require(result["status"] == "passed", "offline evidence gate failed")
    require(binding(args.binding) == checked, "artifacts changed")
    result = dict(protocol=PROTOCOL, status="passed", scope="local independent processes; both CN role assignments",
                  cross_machine_observed=False, network_disconnect_tested=False, binding=checked, directions=results,
                  limitations=["Mac/devbox native network and role swap remain untested", "No physical network disconnection was injected",
                               "No C20 long soak or final-candidate claim", "Temporary test credentials deleted"])
    write_json(args.output / "result.json", result)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("policy", "local", "router", "provider", "client", "control", "verify", "_provider"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--binding", type=Path)
    parser.add_argument("--peer-source-sha", help="Explicit peer build SHA; defaults to the local binding source SHA")
    parser.add_argument("--profile", type=Path)
    parser.add_argument("--request", type=Path)
    parser.add_argument("--control-file", type=Path)
    parser.add_argument("--operation", choices=("down", "up", "stop"))
    parser.add_argument("--client-output", type=Path)
    parser.add_argument("--provider-output", type=Path)
    parser.add_argument("--router-output", type=Path)
    parser.add_argument("--run-id", default="secure_" + uuid.uuid4().hex[:12])
    parser.add_argument("--direction", choices=DIRECTIONS, default="a_to_b")
    parser.add_argument("--domain-id", type=int, default=74)
    parser.add_argument("--stage-timeout", type=float, default=15)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--generation", type=int, default=0, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.peer_source_sha and not re.fullmatch(r"[0-9a-f]{40}", args.peer_source_sha):
        parser.error("--peer-source-sha must be a full lowercase Git SHA")
    require(re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,47}", args.run_id), "run-id must be 1-48 letters/digits/underscores")
    require(0 <= args.domain_id < 2**32 and 0 < args.stage_timeout <= 300 and 0 < args.timeout <= 3600, "invalid bounds")
    if args.mode == "control":
        require(args.control_file is not None and args.operation is not None, "control needs --control-file and --operation")
        write_json(args.control_file, dict(operation=args.operation, nonce=uuid.uuid4().hex))
        return 0
    if args.mode == "verify":
        result = verify(args.client_output, args.provider_output, args.router_output)
        print(json.dumps(result, indent=2))
        return 0 if result["status"] == "passed" else 1
    require(args.output is not None, "--output required")
    args.output = args.output.resolve()
    if args.mode == "policy":
        require(not args.output.exists(), "policy output already exists")
        write_json(args.output, policy(args))
        return 0
    require(args.binding is not None, "--binding required")
    args.binding = args.binding.resolve()
    if args.mode in ("provider", "client", "_provider"):
        require(args.profile is not None and args.profile.is_file(), "--profile required")
        args.profile = args.profile.resolve()
    if args.mode == "router":
        require(args.request is not None and args.control_file is not None, "router needs --request and --control-file")
    if args.mode != "_provider":
        args.output.mkdir(parents=True, exist_ok=False)
    else:
        require(args.generation in (1, 2) and args.output.is_dir(), "invalid internal provider")
    evidence = None
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt("probe interrupted")
    # Public supervisors must run their finally blocks when a caller stops
    # them; otherwise the worker they own could outlive the supervisor.
    signal.signal(signal.SIGTERM, interrupted)
    try:
        if args.mode == "local":
            print(json.dumps(local(args), indent=2))
        else:
            evidence = Evidence(args, args.mode + ("-" + str(args.generation) if args.mode == "_provider" else ""))
            {"provider": provider, "client": client, "router": router, "_provider": provider_worker}[args.mode](args, evidence)
            evidence.close()
        return 0
    except BaseException as error:
        if evidence:
            evidence.emit("failed", error=type(error).__name__, message=str(error))
            evidence.stream.close()
        write_json(args.output / (args.mode + "-failure.json"), dict(status="failed", error=type(error).__name__, message=str(error)))
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
