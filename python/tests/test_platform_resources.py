"""Resource and interruption checks against the installed SDK and real Zenoh.

Each subprocess owns an isolated loopback peer, and the parent only signals
that exact child. These are bounded correctness checks, not a C20 benchmark.
"""

from process_support import console_process_options, interrupt_process, read_line

import json
import os
from pathlib import Path
import queue
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest


ROOT = Path(__file__).resolve().parents[2]
PEER = """{
  mode: "peer",
  connect: { endpoints: [] },
  listen: { endpoints: ["tcp/127.0.0.1:0"] },
  scouting: { multicast: { enabled: false }, gossip: { enabled: false } },
}"""

INTERRUPT_PROGRAM = r"""
import json
from pathlib import Path
import signal
import sys
import threading
import time
import aos_sdk as aos

if hasattr(signal, "SIGBREAK"):
    signal.signal(signal.SIGBREAK, signal.default_int_handler)

config, root, operation, close_in_handler = sys.argv[1:]
close_in_handler = close_in_handler == "1"
context = aos.Context(config=config, domain_id=197)
node = aos.Node("interrupt_wait", "/resources", context)
executor = None
spinner = None
release = threading.Event()
if operation == "service":
    interface = aos.Interface.load(Path(root) / "interfaces/AddTwoInts.json")
    entity = node.create_client(interface, "absent")
    wait = entity.wait_for_service
elif operation == "action":
    interface = aos.Interface.load(Path(root) / "interfaces/action/Fibonacci.json")
    entity = aos.ActionClient(node, interface, "absent")
    wait = entity.wait_for_server
elif operation == "rate":
    node.clock.set_use_sim_time(True)
    entity = node.create_rate(10.0)
    wait = entity.sleep
elif operation == "action_result":
    interface = aos.Interface.load(Path(root) / "interfaces/action/Fibonacci.json")
    executing = threading.Event()
    def execute(goal):
        executing.set()
        assert release.wait(5)
        goal.succeed()
        return {"sequence": [0, 1]}
    provider = aos.ActionServer(node, interface, "result_wait", execute)
    client = aos.ActionClient(node, interface, "result_wait")
    executor = aos.MultiThreadedExecutor(2, context=context)
    executor.add_node(node)
    spinner = threading.Thread(target=executor.spin, daemon=True)
    spinner.start()
    goal = client.send_goal_async({"order": 2}).result(2)
    assert goal.accepted
    assert executing.wait(2)
    entity = goal.get_result_async()
    assert not entity.done()
    wait = entity.result
elif operation == "executor_spin":
    executor = aos.SingleThreadedExecutor(context=context)
    executor.add_node(node)
    wait = executor.spin_once
elif operation == "executor_shutdown":
    executor = aos.MultiThreadedExecutor(1, context=context)
    executor.add_node(node)
    executing = threading.Event()
    def callback():
        executing.set()
        assert release.wait(5)
    timer = node.create_timer(0.001, callback)
    executor.spin_once(1)
    assert executing.wait(2)
    wait = executor.shutdown
else:
    raise AssertionError(operation)
if close_in_handler:
    def interrupt(signum, frame):
        context.shutdown()
        print(json.dumps({"event": "handler_closed"}), flush=True)
        raise KeyboardInterrupt("signal error survives resource close")
    signal.signal(signal.SIGBREAK if hasattr(signal, "SIGBREAK") else signal.SIGINT, interrupt)
print(json.dumps({"event": "waiting", "operation": operation,
                  "main_thread": threading.current_thread() is threading.main_thread()}),
      flush=True)
try:
    wait()
except KeyboardInterrupt as error:
    print(json.dumps({"event": "interrupted", "operation": operation,
                      "message": str(error)}), flush=True)
    if close_in_handler:
        assert not context.ok()
        assert str(error) == "signal error survives resource close"
    elif operation == "service":
        assert not entity.wait_for_service(0)
        provider = node.create_service(interface, "absent", lambda value: {"sum": 0})
        assert entity.wait_for_service(1)
        provider.destroy()
    elif operation == "action":
        assert not entity.wait_for_server(0)
        provider = aos.ActionServer(node, interface, "absent", lambda goal: {"sequence": []})
        assert entity.wait_for_server(1)
        provider.destroy()
    elif operation == "rate":
        node.clock.update_ros_time(0)
        advancing = threading.Thread(
            target=lambda: (time.sleep(0.05), node.clock.update_ros_time(200_000_000))
        )
        advancing.start()
        entity.sleep()
        advancing.join()
    elif operation == "action_result":
        assert not entity.cancelled()
        assert not entity.done()
        release.set()
        assert entity.result(2) == {"status": 4, "result": {"sequence": [0, 1]}}
    elif operation == "executor_spin":
        executor.spin_once(0)
    else:
        release.set()
        assert executor.shutdown(1)
    print(json.dumps({"event": "verified_after_interrupt"}), flush=True)
else:
    raise AssertionError("indefinite wait returned without KeyboardInterrupt")
finally:
    release.set()
    if context.ok():
        context.shutdown()
    if executor is not None:
        assert executor.shutdown(1)
    if spinner is not None:
        spinner.join(1)
        assert not spinner.is_alive()
print(json.dumps({"event": "closed"}), flush=True)
"""


class MainThreadInterruptTest(unittest.TestCase):
    def check_interrupt(self, operation, *, close_in_handler=False):
        with tempfile.TemporaryDirectory(prefix="aos-c19-interrupt-") as directory:
            config = Path(directory) / "peer.json5"
            config.write_text(PEER, encoding="utf-8")
            process = subprocess.Popen(
                [sys.executable, "-I", "-c", INTERRUPT_PROGRAM,
                 str(config), str(ROOT), operation, str(int(close_in_handler))],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, **console_process_options(),
            )
            records = queue.Queue()

            def read():
                for line in process.stdout:
                    records.put(line)

            reader = threading.Thread(target=read, daemon=True)
            reader.start()
            try:
                first = json.loads(records.get(timeout=10))
                self.assertEqual(first["event"], "waiting")
                self.assertTrue(first["main_thread"])
                # The announcement precedes the native call; give the call
                # time to enter its indefinite wait before delivering SIGINT.
                time.sleep(0.15)
                sent = time.monotonic()
                interrupt_process(process, signal.SIGINT)
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    self.fail(f"{operation}: main-thread wait did not handle SIGINT in 2 s")
                self.assertEqual(process.returncode, 0, process.stderr.read())
                reader.join(1)
                remaining = []
                while not records.empty():
                    remaining.append(json.loads(records.get_nowait())["event"])
                expected = ["interrupted", "verified_after_interrupt", "closed"]
                if close_in_handler:
                    expected.insert(0, "handler_closed")
                self.assertEqual(remaining, expected)
                self.assertLess(time.monotonic() - sent, 2)
            finally:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=3)
                reader.join(1)
                process.stdout.close()
                process.stderr.close()

    def test_service_readiness_indefinite_wait_handles_main_thread_sigint(self):
        self.check_interrupt("service")

    def test_action_readiness_indefinite_wait_handles_main_thread_sigint(self):
        self.check_interrupt("action")

    def test_paused_ros_rate_handles_main_thread_sigint(self):
        self.check_interrupt("rate")

    def test_readiness_signal_error_survives_context_close_in_its_handler(self):
        for operation in ("service", "action"):
            with self.subTest(operation=operation):
                self.check_interrupt(operation, close_in_handler=True)

    def test_rate_signal_error_survives_context_close_in_its_handler(self):
        self.check_interrupt("rate", close_in_handler=True)

    def test_public_action_result_future_interrupt_does_not_cancel_the_goal(self):
        self.check_interrupt("action_result")

    def test_executor_indefinite_spin_handles_main_thread_sigint(self):
        self.check_interrupt("executor_spin")

    def test_executor_indefinite_shutdown_handles_main_thread_sigint(self):
        self.check_interrupt("executor_shutdown")
