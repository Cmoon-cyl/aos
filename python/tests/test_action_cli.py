"""Action acceptance: an installed-wheel Fibonacci provider and native CLI.

Required: AOS_ZENOHD (1.10.1) and AOS_CLI (the built Rust executable).
Run from the freshly installed wheel's environment:

    python -I -m unittest discover -s <repo>/python/tests -p test_action_cli.py -v

AOS_ACTION_CLI_RECORD optionally records the child commands, PIDs and output.
"""

import json
import os
from pathlib import Path
import queue
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid

from test_process_echo import read_events, stop_process
from test_stage1_cli import RouterProcess
from process_support import assert_child_identity

ROOT = Path(__file__).resolve().parents[2]
ACTION_TYPE = "aos_examples/action/Fibonacci"
SUCCEEDED = 4
CANCELED = 5

# Execute the actual example, but verify its native extension is installed
# before runpy can execute any repository code.
PROVIDER = r"""
import json, os, pathlib, runpy, site, sys
from aos_sdk import _native
native_path = pathlib.Path(_native.__file__).resolve()
assert sys.flags.isolated
assert any(native_path.is_relative_to(pathlib.Path(p).resolve()) for p in site.getsitepackages()), str(native_path)
print(json.dumps({
    "event": "native", "pid": os.getpid(), "ppid": os.getppid(), "native_path": str(native_path),
    "isolated": bool(sys.flags.isolated),
}), flush=True)
script, config, action = sys.argv[1:]
sys.argv = [script, "--config", config, "--action", action,
            "--delay", "0.1", "--stop-on-stdin"]
runpy.run_path(script, run_name="__main__")
print(json.dumps({"event": "done", "pid": os.getpid()}), flush=True)
"""


def fibonacci(order):
    sequence = []
    for index in range(order):
        sequence.append(index if index < 2 else sequence[-1] + sequence[-2])
    return sequence


class JsonProcess:
    """Read JSON lines while the child is still running, retaining evidence."""

    def __init__(self, command, environment, record, *, interruptible=False):
        self.log = tempfile.TemporaryFile(mode="w+")
        self.lines = []
        self.messages = queue.Queue()
        self.record = record
        self.record["command"] = command
        options = {}
        if interruptible and os.name == "nt":
            options["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        self.process = subprocess.Popen(
            command, env=environment, stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=self.log, text=True, **options,
        )
        self.record["pid"] = self.process.pid
        self.reader = threading.Thread(
            target=read_events,
            args=(self.process.stdout, self.lines, self.messages), daemon=True,
        )
        self.reader.start()

    def diagnostics(self):
        self.log.seek(0)
        return f"pid={self.process.pid}, stdout={self.lines!r}, stderr={self.log.read()!r}"

    def event(self, name, timeout=12):
        deadline = time.monotonic() + timeout
        while True:
            try:
                event = self.messages.get(timeout=max(0, deadline - time.monotonic()))
            except queue.Empty:
                raise AssertionError(f"missing {name}: {self.diagnostics()}") from None
            if event.get("event") == name:
                return event
            if event.get("event") in ("eof", "reader_error"):
                raise AssertionError(f"child ended before {name}: {event!r}; {self.diagnostics()}")

    def send(self, value, *, close=False):
        self.process.stdin.write(value)
        self.process.stdin.flush()
        if close:
            self.process.stdin.close()

    def interrupt(self):
        # Target only the test's own CLI process. Windows console control
        # events require its separate process group; POSIX uses SIGINT.
        self.process.send_signal(
            signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGINT
        )

    def finish(self, expected_exit=0, timeout=12):
        try:
            self.process.wait(timeout)
        except subprocess.TimeoutExpired:
            raise AssertionError(f"child did not finish: {self.diagnostics()}") from None
        self.reader.join(2)
        if self.reader.is_alive():
            raise AssertionError(f"stdout reader did not finish: {self.diagnostics()}")
        self.snapshot()
        if self.process.returncode != expected_exit:
            raise AssertionError(
                f"expected exit {expected_exit}, got {self.process.returncode}: {self.diagnostics()}"
            )
        return [json.loads(line) for line in self.lines]

    def snapshot(self):
        self.log.seek(0)
        self.record.update(
            stdout="".join(self.lines), stderr=self.log.read(),
            exit=self.process.poll(),
        )

    def close(self):
        forced = stop_process(self.process)
        self.reader.join(2)
        self.snapshot()
        self.process.stdin.close()
        self.log.close()
        if forced:
            raise AssertionError(f"child {self.process.pid} needed forced cleanup")


class ActionCliTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.records = []
        for name in ("AOS_ZENOHD", "AOS_CLI"):
            if not os.environ.get(name) or not Path(os.environ[name]).is_file():
                raise AssertionError(f"{name} must name an executable; Action acceptance cannot skip")
        cls.router_version = subprocess.run(
            [os.environ["AOS_ZENOHD"], "--version"], capture_output=True,
            text=True, check=True, timeout=5,
        ).stdout.strip()
        if not re.search(r"\bzenohd v?1\.10\.1\b", cls.router_version):
            raise AssertionError(f"expected zenohd 1.10.1, got {cls.router_version!r}")

    @classmethod
    def tearDownClass(cls):
        destination = os.environ.get("AOS_ACTION_CLI_RECORD")
        if destination:
            Path(destination).write_text(json.dumps({
                "status": "passed" if all(item["status"] == "passed" for item in cls.records) else "failed",
                "router_version": cls.router_version,
                "tests": cls.records,
            }, indent=2) + "\n")

    def setUp(self):
        self.record = {"test": self.id(), "status": "failed", "children": []}
        self.records.append(self.record)
        self.router = RouterProcess()
        self.addCleanup(self.close_process, self.router)
        self.record["router_pid"] = self.router.process.pid
        self.action_name = f"/action_cli_{uuid.uuid4().hex}/fibonacci"
        self.environment = dict(
            os.environ, AOS_ZENOH_CONFIG=str(self.router.config), AOS_DOMAIN_ID="62",
        )
        # CLI type discovery must contact the real provider.
        self.environment.pop("AOS_INTERFACE_PATH", None)
        self.provider = self.child([
            sys.executable, "-I", "-c", PROVIDER,
            str(ROOT / "examples/fibonacci_server.py"), str(self.router.config),
            self.action_name,
        ], "provider")
        native = self.provider.event("native")
        assert_child_identity(self, native, self.provider.process)
        self.assertNotEqual(native["pid"], os.getpid())
        self.assertTrue(native["isolated"])
        self.record["provider_native"] = native
        self.assertEqual(self.provider.event("ready")["action"], self.action_name)

    def close_process(self, process):
        try:
            process.close()
        except BaseException:
            self.record["status"] = "failed"
            raise

    def child(self, command, role, *, interruptible=False):
        record = {"role": role}
        self.record["children"].append(record)
        child = JsonProcess(
            command, self.environment, record, interruptible=interruptible,
        )
        self.addCleanup(self.close_process, child)
        return child

    def cli(self, verb, **parameters):
        request = dict(
            action_name=self.action_name, action_type=ACTION_TYPE, **parameters,
        )
        child = self.child(
            [os.environ["AOS_CLI"], "action", verb, "--request", "-"],
            f"cli:{verb}", interruptible=(verb == "send_goal"),
        )
        child.record["request"] = request
        self.assertNotEqual(child.process.pid, self.provider.process.pid)
        child.send(json.dumps(request) + "\n", close=True)
        return child

    def accepted(self, cli):
        response = cli.event("goal_response")
        self.assertTrue(response["data"]["accepted"], response)
        goal_id = response["data"]["goal_id"]
        self.assertTrue(goal_id)
        return goal_id

    def completed(self, goal_id, status):
        completed = self.provider.event("completed")
        self.assertEqual(completed["goal_id"], goal_id)
        self.assertEqual(completed["status"], status)
        return completed

    def get_result(self, goal_id, previous_cli):
        cli = self.cli("get_result", goal_id=goal_id, timeout=8)
        self.assertNotEqual(cli.process.pid, previous_cli.process.pid)
        output = cli.finish()
        self.assertEqual(len(output), 1, output)
        self.assertTrue(output[0]["ok"], output)
        return output[0]["data"]

    def passed(self):
        self.provider.send("stop\n", close=True)
        self.assertEqual(self.provider.event("done")["pid"], self.record["provider_native"]["pid"])
        self.provider.finish()
        self.record["status"] = "passed"

    def test_send_goal_streams_acceptance_feedback_and_result(self):
        order = 20
        cli = self.cli("send_goal", goal={"order": order}, feedback=True, timeout=8)
        goal_id = self.accepted(cli)
        # Acceptance must be flushed while execution is still in progress.
        self.assertIsNone(cli.process.poll())
        self.assertFalse(any(json.loads(line).get("event") == "completed"
                             for line in list(self.provider.lines)))
        feedback = cli.event("feedback")
        self.assertEqual(feedback["data"]["goal_id"], goal_id)
        partial = feedback["data"]["feedback"]["partial_sequence"]
        self.assertGreater(len(partial), 0)
        self.assertEqual(partial, fibonacci(order)[:len(partial)])
        result = cli.event("result")
        output = cli.finish()
        events = [event["event"] for event in output]
        self.assertEqual(events[0], "goal_response")
        self.assertIn("feedback", events[1:-1])
        self.assertEqual(events[-1], "result")
        self.assertEqual(result["data"], {
            "goal_id": goal_id, "status": SUCCEEDED,
            "result": {"sequence": fibonacci(order)},
        })
        self.assertEqual(self.completed(goal_id, SUCCEEDED)["sequence"], fibonacci(order))
        self.passed()

    def test_timeout_zero_returns_acceptance_then_another_process_gets_result(self):
        order = 20
        cli = self.cli("send_goal", goal={"order": order}, feedback=True, timeout=0)
        goal_id = self.accepted(cli)
        output = cli.finish()
        self.assertEqual([event["event"] for event in output], ["goal_response"])
        self.assertFalse(any(json.loads(line).get("event") == "completed"
                             for line in list(self.provider.lines)))
        result = self.get_result(goal_id, cli)
        self.assertEqual(result, {
            "status": SUCCEEDED, "result": {"sequence": fibonacci(order)},
        })
        self.assertEqual(self.completed(goal_id, SUCCEEDED)["sequence"], fibonacci(order))
        self.passed()

    def test_interrupt_requests_cancel_and_exits_130(self):
        order = 40
        cli = self.cli("send_goal", goal={"order": order}, feedback=True, timeout=10)
        goal_id = self.accepted(cli)
        self.assertEqual(cli.event("feedback")["data"]["goal_id"], goal_id)
        cli.interrupt()
        cancel = cli.event("cancel_response")["data"]
        self.assertEqual(cancel["return_code"], 0)
        self.assertIn(goal_id, [goal["goal_id"] for goal in cancel["goals_canceling"]])
        output = cli.finish(expected_exit=130)
        self.assertEqual(output[0]["event"], "goal_response")
        self.assertEqual(output[-1]["event"], "cancel_response")
        self.assertFalse(any(event["event"] == "error" for event in output))
        result = self.get_result(goal_id, cli)
        self.assertEqual(result["status"], CANCELED)
        sequence = result["result"]["sequence"]
        self.assertLess(len(sequence), order)
        self.assertEqual(sequence, fibonacci(order)[:len(sequence)])
        self.assertEqual(self.completed(goal_id, CANCELED)["sequence"], sequence)
        self.passed()

    def test_explicit_wait_timeout_does_not_cancel_provider(self):
        order = 20
        cli = self.cli("send_goal", goal={"order": order}, feedback=True, timeout=0.2)
        goal_id = self.accepted(cli)
        output = cli.finish(expected_exit=4)
        self.assertEqual(output[0]["event"], "goal_response")
        self.assertEqual(output[-1]["event"], "error")
        self.assertEqual(output[-1]["data"]["code"], "TIMEOUT")
        self.assertFalse(any(event["event"] in ("cancel_response", "result") for event in output))
        # Observe provider completion after the caller has exited; a subsequent
        # independent client must retrieve the successful result.
        self.assertEqual(self.completed(goal_id, SUCCEEDED)["sequence"], fibonacci(order))
        self.assertEqual(self.get_result(goal_id, cli), {
            "status": SUCCEEDED, "result": {"sequence": fibonacci(order)},
        })
        self.passed()


if __name__ == "__main__":
    unittest.main()
