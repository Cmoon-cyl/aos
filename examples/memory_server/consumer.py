"""Use MemoryServer once, then exit. No business request is automatically replayed.

Transport timeouts do not establish whether a put or export committed. Keep the
printed request_id/goal_id and reconcile explicitly with the provider.
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import re
import sys
import threading
import time
import uuid

import aos_sdk as aos

# Only the exact installed sibling module directory is admitted under Python -I.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from request_observer import exporter_from_args


def emit(event, **fields):
    print(json.dumps({"event": event, **fields}, ensure_ascii=False), flush=True)


def bounded_timeout(value):
    number = float(value)
    if not math.isfinite(number) or not 0 < number <= 30:
        raise argparse.ArgumentTypeError("timeout must be finite and in (0, 30] seconds")
    return number


class MemoryClient:
    """A normal Node with bounded waits and explicit one-shot operations."""
    def __init__(self, context, *, namespace="/memory", name="consumer", timeout=10,
                 interface_directory=None, executor=None):
        self.timeout = timeout
        self.namespace = namespace.rstrip("/")
        self.inflight = None
        self._errors = []
        self.node = aos.Node(name, self.namespace, context,
            summary="Explicit one-shot MemoryServer client. Never replays writes or Action goals on reconnect.",
            tags=("memory", "consumer"))
        self.executor = executor
        self.spinner = None
        try:
            self.interfaces = {
                name: aos.Interface.load(
                    Path(interface_directory) / f"{name}.json" if interface_directory is not None
                    else aos.get_package_resource("aos_memory", name.lower()))
                for name in ("Put", "Get", "List", "Status", "Export")
            }
            self.services = {name: self.node.create_client(self.interfaces[name],
                f"{self.namespace}/{name.lower()}") for name in ("Put", "Get", "List")}
            self.action = aos.ActionClient(self.node, self.interfaces["Export"],
                                           f"{self.namespace}/export")
            if self.executor is None:
                self.executor = aos.MultiThreadedExecutor(2, context=context, flush_logs=False)
            self.executor.add_node(self.node)

            def spin():
                try:
                    self.executor.spin()
                except BaseException as error:
                    self._errors.append(type(error).__name__)

            self.spinner = threading.Thread(target=spin, name="memory-consumer-executor")
            self.spinner.start()
        except BaseException:
            self.close()
            raise

    def call(self, operation, request):
        client = self.services[operation]
        if not client.wait_for_service(self.timeout):
            raise TimeoutError("service discovery timed out before submission")
        submission = {"operation": operation.lower()}
        if operation == "Put":
            submission["request_id"] = request["request_id"]
        self.inflight = submission
        emit("service_submitted", **submission)
        # Exactly one call. A lost response never causes an implicit retry.
        result = client.call(request, self.timeout)
        self.inflight = None
        emit("service_result", operation=operation.lower(), result=result)
        return result

    def export(self, request, *, cancel_after_feedback=None):
        if not self.action.wait_for_server(self.timeout):
            raise TimeoutError("Action discovery timed out before submission")
        observed = threading.Event()
        lock = threading.Lock()
        count = 0

        def feedback(value):
            nonlocal count
            with lock:
                count += 1
                observed_count = count
            emit("export_feedback", **value)
            if cancel_after_feedback is not None and observed_count >= cancel_after_feedback:
                observed.set()

        goal_id = uuid.uuid4().hex
        self.inflight = {"operation": "export", "goal_id": goal_id}
        emit("export_submitted", goal_id=goal_id)
        goal = self.action.send_goal_async(request, feedback_callback=feedback,
            goal_id=goal_id, timeout=self.timeout).result(self.timeout + 1)
        if not goal.accepted:
            self.inflight = None
            result = {"goal_id": goal_id, "accepted": False, "terminal": None}
            emit("export_result", **result)
            return result
        result_future = goal.get_result_async(timeout=self.timeout)
        result_future.add_done_callback(lambda _: observed.set())
        cancel_reply = None
        if cancel_after_feedback is not None:
            if not observed.wait(self.timeout + 1):
                raise TimeoutError("no terminal result or requested progress was observed")
            with lock:
                reached = count >= cancel_after_feedback
            if reached:
                # The real export may already have published. Report the actual
                # rejection/terminal status; never manufacture CANCELED.
                cancel_reply = goal.cancel_goal_async(timeout=self.timeout).result(self.timeout + 1)
                emit("export_cancel_result", goal_id=goal_id, result=cancel_reply)
        terminal = result_future.result(self.timeout + 1)
        self.inflight = None
        result = {"goal_id": goal_id, "accepted": True, "terminal": terminal,
                  "cancel_requested": cancel_reply is not None, "feedback_observed": count}
        emit("export_result", **result)
        return result

    def observe(self, count):
        subscription = self.node.create_subscription(self.interfaces["Status"],
                                                      f"{self.namespace}/status")
        values = []
        deadline = time.monotonic() + self.timeout
        try:
            for _ in range(count):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("status observation exceeded its total deadline")
                value = subscription.recv(remaining)
                values.append(value)
                emit("status", message=value)
        finally:
            self.node.destroy_subscription(subscription)
        return values

    def demo(self, request_prefix):
        """Three explicitly named synthetic notes, readback, export and status."""
        prefix = f"synthetic/{request_prefix}/"
        for number in range(3):
            result = self.call("Put", {"request_id": f"{request_prefix}/{number}",
                "key": f"{prefix}{number}", "text": f"synthetic memory note {number}",
                "expected_revision": 0})
            if not result["ok"]:
                return False
        got = self.call("Get", {"key": prefix + "0"})
        listed = self.call("List", {"prefix": prefix, "limit": 10})
        if (not got["ok"] or got["data"]["text"] != "synthetic memory note 0" or
                not listed["ok"] or len(listed["data"]["items"]) != 3):
            return False
        exported = self.export({"prefix": prefix, "max_records": 3})
        if (not exported["accepted"] or exported["terminal"]["status"] != int(aos.GoalStatus.SUCCEEDED)
                or not exported["terminal"]["result"]["ok"]):
            return False
        self.observe(2)
        emit("demo_completed", request_prefix=request_prefix, writes_submitted=3,
             export_goal_id=exported["goal_id"], automatic_replays=0)
        return True

    def close(self):
        stopped = True
        if self.executor is not None:
            stopped = self.executor.shutdown(timeout_sec=self.timeout + 2)
        if self.spinner is not None:
            self.spinner.join(self.timeout + 2)
            stopped = stopped and not self.spinner.is_alive()
        self.node.destroy_node()
        if not stopped or self._errors:
            raise RuntimeError("MemoryServer consumer executor did not finish cleanly")

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    selected = parser.add_mutually_exclusive_group()
    selected.add_argument("--config", type=Path)
    selected.add_argument("--security-profile", type=Path)
    parser.add_argument("--domain-id", type=int)
    parser.add_argument("--namespace", default="/memory", help="Provider endpoint namespace.")
    parser.add_argument("--name", default="consumer")
    parser.add_argument("--interfaces", type=Path, help="Source-development interfaces; installed entries use the package index.")
    parser.add_argument("--telemetry-topic", help="Opt in to real Core request metadata on this absolute Topic; no business payloads.")
    parser.add_argument("--timeout", type=bounded_timeout, default=10)
    commands = parser.add_subparsers(dest="command", required=True)
    put = commands.add_parser("put", help="Submit one explicit request_id once; timeout does not imply rollback.")
    put.add_argument("--request-id", required=True)
    put.add_argument("--key", required=True)
    put.add_argument("--text", required=True)
    put.add_argument("--expected-revision", type=int, default=0)
    get = commands.add_parser("get")
    get.add_argument("--key", required=True)
    listing = commands.add_parser("list")
    listing.add_argument("--prefix", default="")
    listing.add_argument("--after-key", default="")
    listing.add_argument("--limit", type=int, default=50)
    export = commands.add_parser("export", help="Create one complete file on the provider host; no file transfer.")
    export.add_argument("--prefix", default="")
    export.add_argument("--max-records", type=int, default=10_000)
    export.add_argument("--cancel-after-feedback", type=int,
        help="Send exactly one cancel after this many progress callbacks; a fast completed export can reject it.")
    observe = commands.add_parser("observe")
    observe.add_argument("--count", type=int, default=2)
    demo = commands.add_parser("demo", help="Write/read/list/export three synthetic notes, then observe two status samples.")
    demo.add_argument("--request-prefix", required=True,
        help="Stable caller-chosen ASCII identifier (1..64 letters, digits, underscore or hyphen).")
    args = parser.parse_args(argv)
    if not re.fullmatch(r"/(?:[A-Za-z_][A-Za-z0-9_]*)(?:/[A-Za-z_][A-Za-z0-9_]*)*", args.namespace):
        parser.error("namespace must be an absolute non-root Node namespace")
    if args.domain_id is not None and not 0 <= args.domain_id <= 2**32 - 1:
        parser.error("domain-id must be an unsigned 32-bit integer")
    if args.command == "demo" and not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", args.request_prefix):
        parser.error("request-prefix must be a 1..64 character ASCII identifier")
    if args.command == "observe" and not 1 <= args.count <= 100:
        parser.error("count must be in 1..100 and fit the total timeout")
    if args.command == "export" and args.cancel_after_feedback is not None and not 1 <= args.cancel_after_feedback <= 100_000:
        parser.error("cancel-after-feedback must be in 1..100000")
    options = {key: getattr(args, key) for key in ("config", "security_profile", "domain_id")
               if getattr(args, key) is not None}
    client = None
    try:
        with aos.Context(**options) as context:
            executor = aos.MultiThreadedExecutor(2, context=context, flush_logs=False)
            with exporter_from_args(args, context, options, known_executors=(executor,)):
                with MemoryClient(context, namespace=args.namespace, name=args.name, timeout=args.timeout,
                                  interface_directory=args.interfaces, executor=executor) as client:
                    if args.command == "put":
                        success = client.call("Put", {"request_id": args.request_id, "key": args.key,
                            "text": args.text, "expected_revision": args.expected_revision})["ok"]
                    elif args.command == "get":
                        success = client.call("Get", {"key": args.key})["ok"]
                    elif args.command == "list":
                        success = client.call("List", {"prefix": args.prefix, "after_key": args.after_key,
                                                       "limit": args.limit})["ok"]
                    elif args.command == "export":
                        result = client.export({"prefix": args.prefix, "max_records": args.max_records},
                                               cancel_after_feedback=args.cancel_after_feedback)
                        success = result["accepted"] and result["terminal"]["status"] in (
                            int(aos.GoalStatus.SUCCEEDED), int(aos.GoalStatus.CANCELED))
                    elif args.command == "observe":
                        client.observe(args.count)
                        success = True
                    else:
                        success = client.demo(args.request_prefix)
                    return 0 if success else 1
    except Exception as error:
        pending = None if client is None else client.inflight
        emit("consumer_error", error_type=type(error).__name__, pending=pending,
             outcome_uncertain=pending is not None and pending["operation"] in ("put", "export"),
             automatic_replays=0)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
