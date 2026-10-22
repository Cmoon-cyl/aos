#!/usr/bin/env python3
"""Installed-wheel Topic/Service/Action and provider-restart probe.

Run with Python -I. Public modes: provider, client, local. All modes require
an explicit Zenoh config or security profile; no Router or SSH is managed here.
See docs/platform-probe.md for the two-host protocol and evidence contract.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import importlib.machinery
import json
import math
import os
from pathlib import Path
import platform
import re
import signal
import socket
import subprocess
import sys
import sysconfig
import threading
import time
import traceback
import uuid

PROTOCOL = "aos-platform-probe/1"
SCRIPT = Path(__file__).resolve()


def object_schema(**properties):
    return {"type": "object", "properties": properties,
            "required": list(properties), "additionalProperties": False}


STRING = {"type": "string"}
INTEGER = {"type": "integer"}
ORIGIN = object_schema(
    run_id=STRING, instance_id=STRING, round=INTEGER, pid=INTEGER,
    hostname=STRING, node_id=STRING, source_sha=STRING,
    native_sha256=STRING, probe_sha256=STRING,
)
REQUEST = dict(run_id=STRING, instance_id=STRING, request_id=STRING)
DEFINITIONS = {
    "topic": {
        "name": "aos_probe/msg/Tick", "kind": "msg",
        "message": object_schema(origin=ORIGIN, sequence=INTEGER, value=INTEGER),
    },
    "sum": {
        "name": "aos_probe/srv/Add", "kind": "srv",
        "request": object_schema(**REQUEST, a=INTEGER, b=INTEGER),
        "response": object_schema(origin=ORIGIN, request_id=STRING, sum=INTEGER),
    },
    "control": {
        "name": "aos_probe/srv/FinishRound", "kind": "srv",
        "request": object_schema(**REQUEST, operation={"enum": ["restart", "stop"]}),
        "response": object_schema(origin=ORIGIN, request_id=STRING, operation=STRING),
    },
    "action": {
        "name": "aos_probe/action/Accumulate", "kind": "action",
        "goal": object_schema(**REQUEST, steps={"type": "integer", "minimum": 1,
                                               "maximum": 400}),
        "result": {
            **object_schema(origin=ORIGIN, request_id=STRING,
                            completed=INTEGER, total=INTEGER),
            "default": {
                "origin": dict(run_id="", instance_id="", round=0, pid=0, hostname="",
                               node_id="", source_sha="", native_sha256="", probe_sha256=""),
                "request_id": "", "completed": 0, "total": 0,
            },
        },
        "feedback": object_schema(origin=ORIGIN, request_id=STRING,
                                  completed=INTEGER, total=INTEGER),
    },
}


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def config_summary(path):
    raw = path.read_bytes()
    summary = {"path": str(path), "sha256": hashlib.sha256(raw).hexdigest(),
               "bytes": len(raw)}
    # JSON is valid Zenoh JSON5. For general JSON5 keep the digest, rather than
    # heuristically parsing or copying credentials into the evidence.
    try:
        value = json.loads(raw)
        summary.update(mode=value.get("mode"),
                       connect_endpoints=value.get("connect", {}).get("endpoints"),
                       listen_endpoints=value.get("listen", {}).get("endpoints"))
        if "router_endpoints" in value:
            peer = value.get("peer")
            summary.update(mode="peer" if peer else "client",
                           connect_endpoints=value["router_endpoints"],
                           listen_endpoints=[peer["listen_endpoint"]] if peer else [],
                           gossip_enabled=bool(peer))
    except (ValueError, AttributeError, TypeError):
        summary["endpoint_summary"] = "JSON5: supply --network-label for the route description"
    return summary


class Evidence:
    def __init__(self, args):
        self.args = args
        self.lock = threading.Lock()
        self.path = args.output / f"{args.mode}-{args.round}.jsonl"
        self.stream = self.path.open("x", encoding="utf-8")
        self.base = {
            "protocol": PROTOCOL, "run_id": args.run_id, "role": args.mode,
            "pid": os.getpid(), "ppid": os.getppid(), "hostname": socket.gethostname(),
            "platform": platform.platform(), "machine": platform.machine(),
            "python": sys.version, "executable": sys.executable,
            "isolated": bool(sys.flags.isolated), "source_sha": args.source_sha,
            "source_sha_origin": "caller_supplied_build_provenance",
            "build_label": args.build_label, "probe_sha256": digest(SCRIPT),
            "expected_peer_source_sha": getattr(args, "peer_source_sha", None) or args.source_sha,
            "config": config_summary(args.config or args.security_profile),
            "config_kind": "security_profile" if args.security_profile else "native",
            "domain_id": args.domain_id,
            "network_label": args.network_label,
        }
        if args.artifact_sha256:
            self.base["artifact_sha256"] = args.artifact_sha256

    def emit(self, event, **data):
        with self.lock:
            record = {**self.base, "event": event, "unix": time.time(),
                      "monotonic": time.monotonic(), **data}
            line = json.dumps(record, sort_keys=True, allow_nan=False)
            self.stream.write(line + "\n")
            self.stream.flush()
            print(line, flush=True)
        return record


class Deadline:
    def __init__(self, seconds):
        self.end = time.monotonic() + seconds

    def remaining(self):
        value = self.end - time.monotonic()
        if value <= 0:
            raise TimeoutError("probe deadline exceeded")
        return value


def check_origin(origin, args, round_, previous=None):
    require(origin["run_id"] == args.run_id, "received a different run_id")
    require(origin["round"] == round_, "received a different provider round")
    expected_peer = getattr(args, "peer_source_sha", None) or args.source_sha
    require(origin["source_sha"] == expected_peer, "provider source SHA differs")
    require(origin["probe_sha256"] == digest(SCRIPT), "provider probe script differs")
    if previous is not None:
        require(origin == previous, "provider identity changed within a round")


def check_result(response, request, origin, *, canceled):
    require(response["status"] == (5 if canceled else 4), "wrong Action terminal status")
    result = response["result"]
    require(result["origin"] == origin, "Action result came from another provider")
    require(result["request_id"] == request["request_id"], "wrong Action request_id")
    count = result["completed"]
    require(0 <= count <= request["steps"], "invalid Action progress")
    require(result["total"] == count * (count + 1) // 2, "wrong business total")
    require(count < request["steps"] if canceled else count == request["steps"],
            "Action result does not match the requested terminal outcome")


@contextmanager
def runtime(args, evidence, name):
    import aos_sdk as aos
    from aos_sdk import _native

    package = Path(aos.__file__).resolve()
    native = Path(_native.__file__).resolve()
    site = Path(sysconfig.get_paths()["platlib"]).resolve() / "aos_sdk"
    require(sys.flags.isolated, "run this probe with python -I")
    require(native.is_relative_to(site) and package.parent == native.parent,
            "SDK must be installed in this interpreter's site-packages")
    require(isinstance(_native.__loader__, importlib.machinery.ExtensionFileLoader),
            "SDK native module is not a compiled extension")
    evidence.base.update(sdk_version=aos.__version__, sdk_path=str(package),
                         native_path=str(native), native_sha256=digest(native))
    context = aos.Context(config=args.config, security_profile=args.security_profile,
                          domain_id=args.domain_id)
    evidence.base["domain_id"] = context.effective_config["domain_id"]["value"]
    node = aos.Node(name, f"/platform_probe/{args.run_id}", context=context)
    executor = aos.MultiThreadedExecutor(4, context=context)
    executor.add_node(node)
    stopping = threading.Event()
    spinner = threading.Thread(target=executor.spin, name="probe-executor", daemon=True)
    spinner.start()
    try:
        evidence.emit("sdk_ready", node=node.get_node_card())
        yield aos, context, node, executor, stopping
    finally:
        stopping.set()
        if context.ok():
            context.shutdown()
        clean = executor.shutdown(timeout_sec=3)
        spinner.join(timeout=1)
        node.destroy_node()
        require(clean and not spinner.is_alive(), "Executor did not stop cleanly")
        evidence.emit("sdk_closed")


def provider_worker(args, evidence):
    deadline = Deadline(args.overall_timeout)
    with runtime(args, evidence, "provider") as (aos, _, node, _, stopping):
        interfaces = {name: aos.Interface.load(value) for name, value in DEFINITIONS.items()}
        origin = dict(
            run_id=args.run_id, instance_id=uuid.uuid4().hex, round=args.round,
            pid=os.getpid(), hostname=socket.gethostname(),
            node_id=node.get_node_card()["id"], source_sha=args.source_sha,
            native_sha256=evidence.base["native_sha256"],
            probe_sha256=evidence.base["probe_sha256"],
        )
        finish = {}

        def validate(request):
            require(request["run_id"] == args.run_id and
                    request["instance_id"] == origin["instance_id"], "stale request")

        def add(request):
            validate(request)
            response = {"origin": origin, "request_id": request["request_id"],
                        "sum": request["a"] + request["b"]}
            evidence.emit("service_executed", request=request, response=response)
            return response

        def finish_round(request):
            validate(request)
            expected = "restart" if args.round == 1 else "stop"
            require(request["operation"] == expected, "unexpected round operation")
            require(not finish, "round has already been finished")
            # Give the ordinary Service response time to leave before cleanup.
            finish.update(operation=expected, at=time.monotonic() + 0.3)
            evidence.emit("round_finish_requested", request=request, origin=origin)
            return {"origin": origin, "request_id": request["request_id"],
                    "operation": expected}

        def execute(goal):
            request = goal.request
            validate(request)
            completed, total = 0, 0
            evidence.emit("action_started", goal_id=goal.goal_id, request=request, origin=origin)
            for step in range(1, request["steps"] + 1):
                if goal.is_cancel_requested:
                    goal.canceled()
                    break
                if stopping.is_set():
                    goal.abort()
                    break
                completed, total = step, total + step
                goal.publish_feedback({"origin": origin, "request_id": request["request_id"],
                                       "completed": completed, "total": total})
                stopping.wait(0.025)
            else:
                goal.succeed()
            result = {"origin": origin, "request_id": request["request_id"],
                      "completed": completed, "total": total}
            evidence.emit("action_completed", goal_id=goal.goal_id,
                          status=int(goal.status), result=result)
            return result

        publisher = node.create_publisher(interfaces["topic"], "tick")
        node.create_service(interfaces["sum"], "add", add)
        node.create_service(interfaces["control"], "finish_round", finish_round)
        aos.ActionServer(
            node, interfaces["action"], "accumulate", execute,
            goal_callback=lambda request: aos.GoalResponse.ACCEPT
            if request["run_id"] == args.run_id
            and request["instance_id"] == origin["instance_id"] else aos.GoalResponse.REJECT,
            cancel_callback=lambda _: aos.CancelResponse.ACCEPT,
        )
        evidence.emit("provider_ready", origin=origin)
        sequence = 0
        while not finish or time.monotonic() < finish["at"]:
            deadline.remaining()
            publisher.publish({"origin": origin, "sequence": sequence,
                               "value": sequence * 3 + args.round})
            sequence += 1
            stopping.wait(0.05)
        evidence.emit("provider_finished", origin=origin, operation=finish["operation"],
                      published=sequence)


def wait_graph(context, predicate, timeout):
    deadline = Deadline(timeout)
    while True:
        graph = context.graph(timeout=min(0.2, deadline.remaining()))
        if predicate(graph):
            return graph
        time.sleep(min(0.02, deadline.remaining()))


def client_worker(args, evidence):
    with runtime(args, evidence, "client") as (aos, context, node, _, _):
        interfaces = {name: aos.Interface.load(value) for name, value in DEFINITIONS.items()}
        subscription = node.create_subscription(interfaces["topic"], "tick")
        service = node.create_client(interfaces["sum"], "add")
        control = node.create_client(interfaces["control"], "finish_round")
        action = aos.ActionClient(node, interfaces["action"], "accumulate")
        old_origin = None
        for round_ in (1, 2):
            if old_origin is not None:
                graph = wait_graph(context, lambda entries: not any(
                    entry["node_id"] == old_origin["node_id"] for entry in entries
                ), args.stage_timeout)
                evidence.emit("old_provider_gone", old_origin=old_origin, graph=graph)
            require(service.wait_for_service(args.stage_timeout), "Service discovery timed out")
            require(action.wait_for_server(args.stage_timeout), "Action discovery timed out")
            deadline = Deadline(args.stage_timeout)
            # Ignore queued ticks from the previous process, but never count them
            # as evidence for the restarted provider.
            while True:
                tick = subscription.recv(deadline.remaining())
                if tick["origin"]["run_id"] == args.run_id and tick["origin"]["round"] == round_:
                    break
            origin = tick["origin"]
            check_origin(origin, args, round_)
            if old_origin is not None:
                require(origin["instance_id"] != old_origin["instance_id"]
                        and origin["node_id"] != old_origin["node_id"]
                        and origin["pid"] != old_origin["pid"], "provider process did not restart")
            graph = wait_graph(context, lambda entries: any(
                entry["node_id"] == origin["node_id"] and entry["kind"] == "node"
                for entry in entries
            ), args.stage_timeout)
            require(tick["value"] == tick["sequence"] * 3 + round_, "invalid Topic payload")
            evidence.emit("provider_discovered", round=round_, origin=origin, graph=graph)
            evidence.emit("topic_received", round=round_, message=tick)

            def request(**fields):
                return dict(run_id=args.run_id, instance_id=origin["instance_id"],
                            request_id=uuid.uuid4().hex, **fields)

            addition = request(a=19 + round_, b=23)
            response = service.call(addition, timeout=args.stage_timeout)
            require(response == {"origin": origin, "request_id": addition["request_id"],
                                 "sum": addition["a"] + addition["b"]}, "wrong Service result")
            evidence.emit("service_verified", round=round_, request=addition, response=response)
            for canceled in (False, True):
                goal_request = request(steps=400 if canceled else 6)
                goal_id = uuid.uuid4().hex
                received = threading.Event()
                feedbacks = []
                timing_lock = threading.Lock()
                timing = dict(submitted_monotonic=time.monotonic(),
                              accepted_monotonic=None, first_feedback_monotonic=None,
                              first_feedback_count=None, result_done_at_first_feedback=None)
                pending = dict(future=None, requested_monotonic=None)

                def feedback(message, feedbacks=feedbacks, received=received,
                             timing=timing, timing_lock=timing_lock, pending=pending):
                    observed = time.monotonic()
                    with timing_lock:
                        feedbacks.append(message)
                        if timing["first_feedback_monotonic"] is None:
                            timing["first_feedback_monotonic"] = observed
                            timing["first_feedback_count"] = len(feedbacks)
                            future = pending["future"]
                            timing["result_done_at_first_feedback"] = (
                                future.done() if future is not None else None)
                    received.set()

                goal = action.send_goal_async(
                    goal_request, goal_id=goal_id, feedback_callback=feedback,
                    timeout=args.stage_timeout,
                ).result(args.stage_timeout + 1)
                require(goal.accepted, "Action goal rejected")
                with timing_lock:
                    timing["accepted_monotonic"] = time.monotonic()

                def timing_snapshot():
                    with timing_lock:
                        snapshot = dict(timing, feedback_count=len(feedbacks),
                                        result_requested_monotonic=pending["requested_monotonic"])
                        future = pending["future"]
                    first_at = snapshot["first_feedback_monotonic"]
                    snapshot["first_feedback_after_accept_ms"] = (
                        (first_at - snapshot["accepted_monotonic"]) * 1000
                        if first_at is not None else None)
                    # This is a local status-cache observation, not a new query.
                    snapshot["goal_status"] = int(goal.status)
                    snapshot["result_future_done"] = future.done() if future is not None else None
                    if future is not None and future.done():
                        try:
                            snapshot["ready_result_status"] = future.result(0)["status"]
                        except Exception as error:
                            snapshot["ready_result_error"] = f"{type(error).__name__}: {error}"
                    return snapshot

                def request_result():
                    with timing_lock:
                        pending["requested_monotonic"] = time.monotonic()
                    future = goal.get_result_async(timeout=args.stage_timeout)
                    with timing_lock:
                        pending["future"] = future
                    return future

                evidence.emit("action_goal_accepted", round=round_, goal_id=goal_id,
                              request=goal_request, canceled=canceled, timing=timing_snapshot())
                # A successful result releases the feedback registration, as
                # in rclpy. Observe feedback before requesting a short goal's
                # result; independent channels may otherwise deliver result
                # first. Cancellation still exercises an already-pending
                # GetResult while feedback and cancel are in flight.
                result = request_result() if canceled else None
                feedback_received = received.wait(args.stage_timeout)
                evidence.emit("action_feedback_observed" if feedback_received else
                              "action_feedback_timeout", round=round_, goal_id=goal_id,
                              canceled=canceled, timing=timing_snapshot())
                require(feedback_received, "no Action feedback received")
                first = feedbacks[0]
                require(first["goal_id"] == goal.goal_id
                        and first["feedback"]["origin"] == origin
                        and first["feedback"]["request_id"] == goal_request["request_id"],
                        "feedback belongs to another goal/provider")
                if canceled:
                    cancel = goal.cancel_goal_async(timeout=args.stage_timeout).result(
                        args.stage_timeout + 1
                    )
                    require(goal.goal_id in [item["goal_id"] for item in cancel["goals_canceling"]],
                            "requested goal is absent from goals_canceling")
                    evidence.emit("cancel_accepted", round=round_, goal_id=goal.goal_id,
                                  response=cancel)
                if result is None:
                    result = request_result()
                try:
                    response = result.result(args.stage_timeout + 1)
                except Exception:
                    evidence.emit("action_result_failed", round=round_, goal_id=goal_id,
                                  canceled=canceled, timing=timing_snapshot())
                    raise
                with timing_lock:
                    timing["result_observed_monotonic"] = time.monotonic()
                check_result(response, goal_request, origin, canceled=canceled)
                evidence.emit("action_cancel_verified" if canceled else "action_success_verified",
                              round=round_, goal_id=goal.goal_id, request=goal_request,
                              response=response, first_feedback=first, timing=timing_snapshot())
            command = request(operation="restart" if round_ == 1 else "stop")
            response = control.call(command, timeout=args.stage_timeout)
            require(response == {"origin": origin, "request_id": command["request_id"],
                                 "operation": command["operation"]}, "wrong control response")
            evidence.emit("round_verified", round=round_, origin=origin)
            old_origin = origin
        graph = wait_graph(context, lambda entries: not any(
            entry["node_id"] == old_origin["node_id"] for entry in entries
        ), args.stage_timeout)
        evidence.emit("final_provider_gone", old_origin=old_origin, graph=graph)


def child_command(args, mode, output, round_=0, *, remaining=None):
    command = [
        sys.executable, "-I", "-u", str(SCRIPT), mode,
        "--output", str(output),
        "--run-id", args.run_id, "--source-sha", args.source_sha,
        "--build-label", args.build_label,
        "--stage-timeout", str(args.stage_timeout),
        "--overall-timeout", str(remaining or args.overall_timeout),
        "--restart-delay", str(args.restart_delay), "--round", str(round_),
        "--network-label", args.network_label,
    ]
    command.extend(["--security-profile", str(args.security_profile)] if args.security_profile
                   else ["--config", str(args.config)])
    if args.domain_id is not None:
        command.extend(["--domain-id", str(args.domain_id)])
    if args.artifact_sha256:
        command.extend(["--artifact-sha256", args.artifact_sha256])
    if getattr(args, "peer_source_sha", None):
        command.extend(["--peer-source-sha", args.peer_source_sha])
    return command


def stop_process(process, *, group=False):
    if process.poll() is not None and not (group and os.name == "posix"):
        return
    try:
        if group and os.name == "posix":
            os.killpg(process.pid, signal.SIGTERM)
        else:
            process.terminate()
        process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        if group and os.name == "posix":
            os.killpg(process.pid, signal.SIGKILL)
        else:
            process.kill()
        process.wait(timeout=3)
    except ProcessLookupError:
        process.wait(timeout=3)
    finally:
        if group and os.name == "posix":
            # A supervisor can die before its worker. Kill only its own process
            # group even when the group leader has already been reaped.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


def supervised_worker(args, evidence, mode, round_, deadline):
    command = child_command(args, mode, args.output, round_, remaining=deadline.remaining())
    with (args.output / f"{mode}-{round_}.stdout").open("xb") as log:
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                   stdin=subprocess.DEVNULL)
        evidence.emit("process_started", child_pid=process.pid, command=command)
        try:
            result = process.wait(timeout=deadline.remaining())
            require(result == 0, f"{mode} round {round_} exited {result}; see {log.name}")
        finally:
            stop_process(process)
            evidence.emit("process_exited", child_pid=process.pid, returncode=process.returncode)


def local_pair(args, evidence):
    deadline = Deadline(args.overall_timeout)
    children = []
    try:
        for mode in ("provider", "client"):
            command = child_command(args, mode, args.output / mode)
            with (args.output / f"{mode}.stdout").open("xb") as log:
                process = subprocess.Popen(
                    command, stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                    start_new_session=os.name == "posix",
                )
            children.append(process)
            evidence.emit("process_started", role_started=mode,
                          child_pid=process.pid, command=command)
        while any(process.poll() is None for process in children):
            deadline.remaining()
            for process in children:
                require(process.poll() in (None, 0), f"child {process.pid} failed; see child evidence")
            time.sleep(0.05)
        require(all(process.returncode == 0 for process in children), "local probe failed")
    finally:
        for process in children:
            stop_process(process, group=True)
            evidence.emit("process_exited", child_pid=process.pid, returncode=process.returncode)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["provider", "client", "local", "_provider", "_client"],
                        metavar="{provider,client,local}")
    network = parser.add_mutually_exclusive_group(required=True)
    network.add_argument("--config", type=Path)
    network.add_argument("--security-profile", type=Path)
    parser.add_argument("--output", type=Path, required=True, help="New or empty evidence directory")
    parser.add_argument("--run-id", required=True, help="Shared across both hosts; unique per run")
    parser.add_argument("--source-sha", required=True, help="Installed artifact's declared Git SHA")
    parser.add_argument("--peer-source-sha",
                        help="Explicit provider SHA for a mixed-version compatibility probe; "
                             "defaults to --source-sha")
    parser.add_argument("--artifact-sha256", help="Optional wheel SHA256 from its build manifest")
    parser.add_argument("--build-label", default="candidate", choices=["baseline", "candidate"])
    parser.add_argument("--network-label", default="", help="Operator-supplied route/topology label")
    parser.add_argument("--domain-id", type=int,
                        help="Defaults to the security profile domain, or 72 with --config")
    parser.add_argument("--stage-timeout", type=float, default=10)
    parser.add_argument("--overall-timeout", type=float, default=120)
    parser.add_argument("--restart-delay", type=float, default=1)
    parser.add_argument("--round", type=int, default=0, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,63}", args.run_id):
        parser.error("--run-id must begin with a letter and contain at most 64 letters/digits/_")
    if not re.fullmatch(r"[0-9a-f]{40}", args.source_sha):
        parser.error("--source-sha must be a full lowercase Git SHA")
    if args.peer_source_sha and not re.fullmatch(r"[0-9a-f]{40}", args.peer_source_sha):
        parser.error("--peer-source-sha must be a full lowercase Git SHA")
    if args.artifact_sha256 and not re.fullmatch(r"[0-9a-f]{64}", args.artifact_sha256):
        parser.error("--artifact-sha256 must be a lowercase SHA256")
    for name in ("stage_timeout", "overall_timeout", "restart_delay"):
        value = getattr(args, name)
        if not math.isfinite(value) or value <= 0 or value > 3600:
            parser.error(f"--{name.replace('_', '-')} must be finite and in (0, 3600]")
    if args.domain_id is None and args.config is not None:
        args.domain_id = 72
    if args.domain_id is not None and not 0 <= args.domain_id < 2**32:
        parser.error("--domain-id must fit uint32")
    args.output = args.output.resolve()
    for field in ("config", "security_profile"):
        value = getattr(args, field)
        if value is not None:
            value = value.resolve()
            if not value.is_file():
                parser.error(f"--{field.replace('_', '-')} must name an existing file")
            setattr(args, field, value)
    if args.mode == "_provider" and args.round not in (1, 2):
        parser.error("internal provider round must be 1 or 2")
    return args


def main(argv=None):
    args = parse_args(argv)
    args.output.mkdir(parents=True, exist_ok=True)
    if not args.mode.startswith("_") and any(args.output.iterdir()):
        raise SystemExit(f"Output directory must be empty: {args.output}")
    evidence = Evidence(args)
    status, error = "passed", None
    started = time.monotonic()
    # Public processes own their child cleanup. Workers have a separate hard
    # deadline imposed by their parent even if native open/close hangs.
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt("probe interrupted")
    signal.signal(signal.SIGTERM, interrupted)
    try:
        require(sys.flags.isolated, "run this probe with python -I")
        evidence.emit("started")
        if args.mode == "_provider":
            provider_worker(args, evidence)
        elif args.mode == "_client":
            client_worker(args, evidence)
        elif args.mode == "local":
            local_pair(args, evidence)
        else:
            deadline = Deadline(args.overall_timeout)
            rounds = (1, 2) if args.mode == "provider" else (0,)
            for round_ in rounds:
                supervised_worker(args, evidence, "_" + args.mode, round_, deadline)
                if args.mode == "provider" and round_ == 1:
                    time.sleep(min(args.restart_delay, deadline.remaining()))
    except BaseException as caught:
        status, error = "failed", f"{type(caught).__name__}: {caught}"
        traceback.print_exc(file=sys.stderr)
    finally:
        record = evidence.emit("finished", status=status, error=error,
                               elapsed_seconds=time.monotonic() - started)
        summary = args.output / f"{args.mode}-{args.round}.json"
        summary.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
        evidence.stream.close()
    return 0 if status == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
