"""Real SDK lifecycle, services and Action states over an isolated local session.

These are same-process integration tests. Installed process, TLS and cross-host
acceptance are separate; no default router or user data is used here.
"""

import os
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import threading
import unittest
import uuid

import aos_sdk as aos

EXAMPLE = Path(__file__).resolve().parents[2] / "examples/memory_server"
sys.path.insert(0, str(EXAMPLE))
from memory_store import MemoryStore
from provider import MemoryNode
from consumer import MemoryClient


class MemoryServerTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="aos-memory-sdk-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.config = self.directory / "isolated.json"
        self.config.write_text(json.dumps({
            "mode": "peer", "connect": {"endpoints": []}, "listen": {"endpoints": []},
            "scouting": {"multicast": {"enabled": False}, "gossip": {"enabled": False}},
        }))
        self.context = aos.Context(config=self.config)
        self.addCleanup(self.context.shutdown)
        self.store = MemoryStore(self.directory / "data")
        self.addCleanup(self.store.close)
        self.namespace = "/memory_" + uuid.uuid4().hex
        self.published = threading.Event()
        published = self.published

        class ObservedMemoryNode(MemoryNode):
            def _publish_status(self):
                result = super()._publish_status()
                if result:
                    published.set()
                return result

        self.node = ObservedMemoryNode(self.store, context=self.context, namespace=self.namespace,
                                       interface_directory=EXAMPLE / "interfaces")
        self.addCleanup(self.node.destroy_node)
        self.client = aos.Node("consumer", self.namespace, self.context)
        self.addCleanup(self.client.destroy_node)
        self.services = {name: self.client.create_client(self.node.interfaces[name], name.lower())
                         for name in ("Put", "Get", "List")}
        self.action = aos.ActionClient(self.client, self.node.interfaces["Export"], "export")
        self.executor = aos.MultiThreadedExecutor(6, context=self.context)
        self.executor.add_node(self.node)
        self.executor.add_node(self.client)
        self.thread_errors = []

        def spin():
            try:
                self.executor.spin()
            except BaseException as error:
                self.thread_errors.append(error)

        self.spinner = threading.Thread(target=spin, name="memory-sdk-test-executor")
        self.spinner.start()
        self.addCleanup(self.close_executor)

    def close_executor(self):
        self.node.request_stop()
        stopped = self.executor.shutdown(timeout_sec=4)
        self.spinner.join(4)
        self.assertTrue(stopped)
        self.assertFalse(self.spinner.is_alive())
        self.assertEqual(self.thread_errors, [])

    def activate(self):
        self.assertTrue(self.node.trigger_configure_async().result(2)["successful"])
        self.assertTrue(self.node.trigger_activate_async().result(2)["successful"])

    def put(self, key="synthetic/note", text="synthetic business note", **extra):
        request = {"request_id": uuid.uuid4().hex, "key": key, "text": text, **extra}
        return self.services["Put"].call(request, 2)

    def release_on_cleanup(self, *events):
        for event in events:
            self.addCleanup(event.set)

    def test_active_provider_publishes_without_a_status_consumer_and_exposes_contracts(self):
        self.activate()
        # No Status subscription has been created. This observes successful
        # publication, not a claim of delivery to a nonexistent consumer.
        self.assertTrue(self.published.wait(2))
        self.assertEqual(self.store.stats()["records"], 0)
        audit = (self.store.directory / "audit.jsonl").read_text()
        self.assertIn('"event":"status_published"', audit)
        card = self.node.get_node_card()
        encoded = json.dumps(card)
        for word in ("request_id", "expected_revision", "export", "1.0.0"):
            self.assertIn(word, encoded)
        subscription = self.client.create_subscription(self.node.interfaces["Status"], "status")
        value = subscription.recv(2)
        self.assertEqual(value["instance_id"], self.store.instance_id)
        self.assertGreaterEqual(value["sequence"], 2)
        self.assertNotIn("text", value)

    def test_inactive_business_is_rejected_then_real_services_commit_and_read(self):
        with self.assertRaises(aos.InactiveError):
            self.put()
        rejected = self.action.send_goal_async({}).result(2)
        self.assertFalse(rejected.accepted)
        self.activate()
        first = self.put(request_id="once")
        second = self.put(request_id="once")
        self.assertTrue(first["ok"], first)
        self.assertEqual(second, first)
        self.assertEqual(self.store.stats()["commit_seq"], 1)
        got = self.services["Get"].call({"key": "synthetic/note"}, 2)
        self.assertEqual(got["data"]["text"], "synthetic business note")
        listed = self.services["List"].call({"prefix": "synthetic/"}, 2)
        self.assertEqual([item["key"] for item in listed["data"]["items"]], ["synthetic/note"])
        self.assertEqual(self.services["Get"].call({"key": "absent"}, 2)["code"], "NOT_FOUND")

    def test_capacity_parameters_require_inactive_and_cannot_invalidate_committed_data(self):
        self.activate()
        self.assertTrue(self.put(text="four")["ok"])
        changed = self.node.set_parameters_atomically([aos.Parameter("max_record_bytes", value=8)])
        self.assertFalse(changed.successful)
        self.assertTrue(self.node.trigger_deactivate_async(drain_timeout=1).result(2)["successful"])
        rejected = self.node.set_parameters_atomically([
            aos.Parameter("max_record_bytes", value=3), aos.Parameter("export_batch_size", value=1),
        ])
        self.assertFalse(rejected.successful)
        self.assertEqual(self.node.get_parameter("export_batch_size").value, 64)
        self.assertTrue(self.node.set_parameters_atomically([
            aos.Parameter("max_record_bytes", value=8), aos.Parameter("export_batch_size", value=1),
        ]).successful)
        self.assertTrue(self.node.trigger_activate_async().result(2)["successful"])
        self.assertEqual(self.put("large", "too large for eight")["code"], "CAPACITY")

    def test_export_returns_native_succeeded_and_hash_of_the_actual_file(self):
        self.activate()
        self.assertTrue(self.put()["ok"])
        feedback = []
        received_feedback = threading.Event()

        def on_feedback(message):
            feedback.append(message)
            received_feedback.set()

        goal = self.action.send_goal_async({}, feedback_callback=on_feedback).result(2)
        self.assertTrue(goal.accepted)
        # Feedback and GetResult are independent channels. Observe feedback
        # before reading the result, which releases its callback registration.
        self.assertTrue(received_feedback.wait(2))
        result = goal.get_result_async(timeout=3).result(4)
        self.assertEqual(result["status"], int(aos.GoalStatus.SUCCEEDED), result)
        payload = result["result"]["data"]
        content = (self.store.directory / payload["file"]).read_bytes()
        self.assertEqual(hashlib.sha256(content).hexdigest(), payload["sha256"])
        self.assertEqual(json.loads(content)["text"], "synthetic business note")
        self.assertTrue(feedback)
        self.assertEqual(feedback[-1]["feedback"]["records_written"], 1)
        self.assertEqual(self.store.stats()["active_exports"], 0)

    def test_cancel_intent_blocks_publication_before_sdk_canceling_and_finishes_canceled(self):
        at_commit, release_commit = threading.Event(), threading.Event()
        at_decision, release_decision = threading.Event(), threading.Event()
        self.release_on_cleanup(release_commit, release_decision)
        observed = {}

        def before_publish(publication):
            at_commit.set()
            if not release_commit.wait(3):
                raise RuntimeError("test commit gate was not released")

        def cancel_decision(goal, publication):
            observed.update(goal=goal, publication=publication)
            at_decision.set()
            if not release_decision.wait(2):
                raise RuntimeError("test cancel decision gate was not released")

        self.node._before_publish = before_publish
        self.node._cancel_decision_gate = cancel_decision
        self.activate()
        self.put()
        goal = self.action.send_goal_async({}).result(2)
        result = goal.get_result_async(timeout=5)
        self.assertTrue(at_commit.wait(2))
        cancel = goal.cancel_goal_async(timeout=3)
        self.assertTrue(at_decision.wait(2))
        self.assertFalse(observed["goal"].is_cancel_requested)
        release_commit.set()
        self.assertTrue(observed["publication"].closed_event.wait(1))
        self.assertEqual(list(self.store.exports.iterdir()), [])
        self.assertFalse(observed["goal"].is_cancel_requested)
        release_decision.set()
        self.assertEqual([entry["goal_id"] for entry in cancel.result(3)["goals_canceling"]], [goal.goal_id])
        terminal = result.result(4)
        self.assertEqual(terminal["status"], int(aos.GoalStatus.CANCELED), terminal)
        self.assertEqual(terminal["result"]["code"], "CANCELED")
        self.assertEqual(list(self.store.exports.iterdir()), [])

    def test_cancel_after_publication_is_rejected_before_native_success(self):
        published, release = threading.Event(), threading.Event()
        self.release_on_cleanup(release)

        def after_publish(publication):
            self.assertTrue(publication.published)
            published.set()
            if not release.wait(3):
                raise RuntimeError("test published gate was not released")

        self.node._after_publish = after_publish
        self.activate()
        self.put()
        goal = self.action.send_goal_async({}).result(2)
        result = goal.get_result_async(timeout=5)
        self.assertTrue(published.wait(2))
        cancellation = goal.cancel_goal_async(timeout=2).result(3)
        self.assertEqual(cancellation["goals_canceling"], [])
        release.set()
        terminal = result.result(4)
        self.assertEqual(terminal["status"], int(aos.GoalStatus.SUCCEEDED), terminal)
        self.assertTrue((self.store.directory / terminal["result"]["data"]["file"]).is_file())

    def test_one_shot_consumer_completes_real_business_without_repeating_writes(self):
        self.activate()
        with MemoryClient(self.context, namespace=self.namespace, name="ordinary_client", timeout=4,
                          interface_directory=EXAMPLE / "interfaces") as client:
            self.assertTrue(client.demo("ordinary-client"))
        self.assertEqual(self.store.stats()["commit_seq"], 3)
        self.assertEqual(self.store.stats()["requests"], 3)
        self.assertEqual(len(list(self.store.exports.glob("*.jsonl"))), 1)

    def test_committed_put_timeout_does_not_trigger_a_consumer_replay(self):
        committed, release = threading.Event(), threading.Event()
        self.release_on_cleanup(release)
        original = self.store.put
        calls = []

        def delayed_response(request, limits):
            calls.append(request["request_id"])
            result = original(request, limits)
            committed.set()
            if not release.wait(2):
                raise RuntimeError("test response gate was not released")
            return result

        self.store.put = delayed_response
        self.activate()
        with MemoryClient(self.context, namespace=self.namespace, name="one_shot", timeout=0.15,
                          interface_directory=EXAMPLE / "interfaces") as client:
            request = {"request_id": "lost-response", "key": "synthetic/timeout", "text": "committed once"}
            try:
                with self.assertRaises(aos.WaitTimeoutError):
                    client.call("Put", request)
                self.assertTrue(committed.wait(1))
                self.assertEqual(calls, ["lost-response"])
                self.assertEqual(client.inflight, {"operation": "put", "request_id": "lost-response"})
                self.assertEqual(self.store.stats()["commit_seq"], 1)
            finally:
                release.set()
            # Only this explicit caller action repeats the same request_id.
            client.timeout = 2
            recovered = client.call("Put", request)
            self.assertTrue(recovered["ok"])
            self.assertEqual(calls, ["lost-response", "lost-response"])
            self.assertEqual(self.store.stats()["commit_seq"], 1)


if __name__ == "__main__":
    unittest.main()
