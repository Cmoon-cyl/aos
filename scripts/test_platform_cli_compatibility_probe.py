"""Reject plausible but insufficient CLI compatibility evidence without network I/O."""

import copy
import importlib.util
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location(
    "cli_compatibility", Path(__file__).with_name("platform_cli_compatibility_probe.py"))
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)


def fixture():
    origin = dict(run_id="run", instance_id="provider", round=1, pid=42, hostname="host",
                  node_id="node", source_sha="a" * 40, native_sha256="b" * 64,
                  probe_sha256="c" * 64)
    provider, commands, requests = [], {}, {}

    def emit(name, **data):
        return dict(event=name, **data)

    def output(name, **data):
        return dict(event=name, data=data)

    def response(value):
        return [{"ok": True, "data": value}]

    def request(id_, **values):
        return dict(run_id="run", instance_id="provider", request_id=id_, **values)

    message = dict(origin=origin, sequence=4, value=13)
    provider.append(emit("topic_published", message=message))
    commands["topic-echo"] = [output("message", message=message)]
    for mode, value in (("default", 17), ("once", -9)):
        sent = request(mode, value=value)
        requests["topic-pub-" + mode] = dict(values=sent, topic_name="/test/ingress")
        if mode == "once":
            requests["topic-pub-once"]["once"] = True
        commands["topic-pub-" + mode] = [output("published", sequence=1, submitted=True,
                                                       topic_name="/test/ingress")]
        provider.append(emit("ingress_applied", request=sent, count=1, applied_value=value * 7))
        receipt = request("receipt-" + mode, topic_request_id=mode)
        requests["receipt-" + mode] = {"values": receipt}
        received = dict(origin=origin, request_id=receipt["request_id"], topic_request_id=mode,
                        received_count=1, input_value=value, applied_value=value * 7)
        provider.append(emit("receipt_executed", request=receipt, response=received))
        commands["receipt-" + mode] = response({"response": received})
    addition = request("sum", a=29, b=-8)
    added = dict(origin=origin, request_id="sum", sum=21)
    requests["service-call"] = {"values": addition}
    commands["service-call"] = response({"response": added})
    provider.append(emit("service_executed", request=addition, response=added))
    for mode, goal, steps, count, status in (("success", "d" * 32, 4, 4, 4),
                                            ("cancel", "e" * 32, 40, 2, 5)):
        label = "action-" + mode
        asked = request(mode + "-goal", steps=steps)
        requests[label] = {"goal": asked}
        provider.append(emit("action_started", goal_id=goal, request=asked))
        commands[label] = [output("goal_response", goal_id=goal, accepted=True)]
        for completed in range(1, count + 1):
            feedback = dict(origin=origin, request_id=asked["request_id"], completed=completed,
                            total=completed * (completed + 1) // 2)
            provider.append(emit("action_feedback", goal_id=goal, feedback=feedback))
            commands[label].append(output("feedback", goal_id=goal, feedback=feedback))
        result = dict(origin=origin, request_id=asked["request_id"], completed=count,
                      total=count * (count + 1) // 2)
        provider.append(emit("action_completed", goal_id=goal, status=status, result=result))
        commands[label].append(output("result", goal_id=goal, status=status, result=result))
        requests[label + "-get-result"] = {"goal_id": goal}
        commands[label + "-get-result"] = response({"status": status, "result": result})
        if mode == "cancel":
            provider.append(emit("cancel_requested", goal_id=goal, request=asked))
            requests["action-cancel-request"] = {"goal_id": goal}
            commands["action-cancel-request"] = response(dict(
                return_code=0, goals_canceling=[{"goal_id": goal}]))
    # No cross-object aliases: modifying one side must not alter the other side
    # of a comparison, as with independent files from two real processes.
    return tuple(copy.deepcopy(value) for value in (provider, commands, requests, origin))


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.provider, self.commands, self.requests, self.origin = fixture()

    def verify(self):
        return probe.validate_effects(self.provider, self.commands, self.requests, self.origin)

    def test_valid_business_evidence(self):
        result = self.verify()
        self.assertEqual(result["received_and_applied"], 2)
        self.assertEqual(result["service_executions"], 1)
        self.assertEqual(len(result["unique_request_ids"]), 7)

    def test_submission_and_matching_do_not_prove_delivery(self):
        self.provider = [row for row in self.provider if row["event"] != "ingress_applied"]
        with self.assertRaisesRegex(RuntimeError, "provider ingress"):
            self.verify()

    def test_duplicate_actual_topic_delivery_is_rejected(self):
        self.provider.append(copy.deepcopy(next(row for row in self.provider
                                                if row["event"] == "ingress_applied")))
        with self.assertRaisesRegex(RuntimeError, "provider ingress"):
            self.verify()

    def test_published_counter_must_be_exactly_one(self):
        self.commands["topic-pub-default"][0]["data"]["sequence"] = 2
        with self.assertRaisesRegex(RuntimeError, "single CLI publication"):
            self.verify()

    def test_explicit_many_cannot_be_reported_as_default_once(self):
        self.requests["topic-pub-default"]["times"] = 3
        with self.assertRaisesRegex(RuntimeError, "requested mode"):
            self.verify()

    def test_receipt_for_a_different_request_is_rejected(self):
        self.requests["receipt-once"]["values"]["topic_request_id"] = "default"
        with self.assertRaisesRegex(RuntimeError, "receipt targets"):
            self.verify()

    def test_wrong_service_value_even_with_success_exit_is_rejected(self):
        self.commands["service-call"][0]["data"]["response"]["sum"] = 22
        with self.assertRaisesRegex(RuntimeError, "Service business"):
            self.verify()

    def test_duplicate_service_execution_is_rejected(self):
        self.provider.append(copy.deepcopy(next(row for row in self.provider
                                                if row["event"] == "service_executed")))
        with self.assertRaisesRegex(RuntimeError, "service_executed"):
            self.verify()

    def test_cancel_acceptance_without_final_canceled_is_rejected(self):
        for row in self.provider:
            if row["event"] == "action_completed" and row["goal_id"] == "e" * 32:
                row["status"] = 3
        self.commands["action-cancel"][-1]["data"]["status"] = 3
        with self.assertRaisesRegex(RuntimeError, "terminal status"):
            self.verify()

    def test_cancel_code_zero_without_the_requested_goal_is_rejected(self):
        self.commands["action-cancel-request"][0]["data"]["goals_canceling"] = []
        with self.assertRaisesRegex(RuntimeError, "CancelGoal"):
            self.verify()

    def test_result_from_another_goal_is_rejected(self):
        self.requests["action-success-get-result"]["goal_id"] = "f" * 32
        with self.assertRaisesRegex(RuntimeError, "GetResult"):
            self.verify()

    def test_feedback_must_be_observed_by_cli(self):
        self.commands["action-success"] = [row for row in self.commands["action-success"]
                                           if row["event"] != "feedback"]
        with self.assertRaisesRegex(RuntimeError, "feedback was not observed"):
            self.verify()

    def test_feedback_requires_corresponding_provider_event(self):
        self.provider = [row for row in self.provider if not (row["event"] == "action_feedback"
                         and row["goal_id"] == "d" * 32 and row["feedback"]["completed"] == 2)]
        with self.assertRaisesRegex(RuntimeError, "provider feedback"):
            self.verify()

    def test_reused_business_request_id_is_rejected(self):
        self.requests["service-call"]["values"]["request_id"] = "default"
        with self.assertRaisesRegex(RuntimeError, "request ID was reused"):
            self.verify()


if __name__ == "__main__":
    unittest.main()
