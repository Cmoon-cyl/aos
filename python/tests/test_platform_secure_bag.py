"""Installed Python/CLI Bag boundaries; native secure_bag adds ACL wire traces.

The two runners share one exact policy fixture and the shipped TLS Router.
Neither local publish counters nor discovered channel metadata prove delivery.
"""
from process_support import console_process_options, interrupt_process, read_line

import json
import os
from pathlib import Path
import signal
import time
import unittest

import aos_sdk as aos
from process_support import interrupt_process
import test_platform_bag as bag_cases
import test_platform_security as security_cases
from test_platform_foundation import ROOT


class SecureBagBindingsTest(unittest.TestCase):
    # Reuse fixture/process helpers without inheriting their unrelated tests.
    tls_files = security_cases.SecurityBindingTest.tls_files
    client = security_cases.SecurityBindingTest.client
    cli = security_cases.SecurityBindingTest.cli
    start_router = security_cases.SecurityBindingTest.start_router
    context = security_cases.SecurityBindingTest.context
    start_bag = bag_cases.BagCliTest.start
    event = bag_cases.BagCliTest.event
    wait_for = bag_cases.BagCliTest.wait_for

    @classmethod
    def setUpClass(cls):
        security_cases.SecurityBindingTest.setUpClass.__func__(cls)

    @classmethod
    def tearDownClass(cls):
        security_cases.SecurityBindingTest.tearDownClass.__func__(cls)

    def setUp(self):
        self.result_path = os.environ.get("AOS_SECURE_BAG_RESULT")
        security_cases.SecurityBindingTest.setUp(self)
        self.policy = json.loads((ROOT / "python/tests/fixtures/secure_bag_policy.json").read_text())
        self.count = 8

    @staticmethod
    def node_options(target=None):
        program = {"name": "operator", "namespace": "/secure"}
        if target is not None:
            program["remaps"] = [{"kind": "topic", "from": "/replay/secure/allowed", "to": target}]
        return {"program": program}

    def checked_closure(self, status):
        self.assertEqual(status["state"], "finished", status)
        self.assertTrue(status["resources_closed"], status)
        self.assertIsNone(status["cleanup_error"], status)
        self.assertIsNone(status["error"], status)

    def record(self, entry, profile, context, allowed, denied, denied_control):
        directory = self.case / f"{entry}-recording"
        options = {
            "topics": ["/secure/allowed", "/secure/denied"],
            "node": self.node_options(), "discovery_interval_ms": 20,
            "description_timeout_ms": 1000,
        }
        if entry == "python":
            recorder = aos.BagRecorder(context, directory, **options)
            self.addCleanup(recorder.close)
            self.wait_for(lambda: len(recorder.status["active_channels"]) == 2)
            self.assertEqual(recorder.status["unresolved"], [])
        else:
            options["node"] = {"cli": options["node"]["program"]}
            process, events, _ = self.start_bag("record", options | {
                "directory": str(directory), "security_profile": str(profile),
                "status_interval": 0.02,
            })
            active = self.event(events, "recording_status", lambda event:
                                event["data"]["active_channel_count"] == 2)
            self.assertEqual(active["unresolved_count"], 0)
        # Only the recorder has a subscription to the allowed source.
        allowed.wait_for_subscribers(3)
        expected = [f"{entry}-record-allowed-{index}" for index in range(self.count)]
        for index, value in enumerate(expected):
            allowed.publish({"data": value})
            marker = f"{entry}-record-denied-{index}"
            denied.publish({"data": marker})
            self.assertEqual(denied_control.recv(3), {"data": marker})
        if entry == "python":
            self.wait_for(lambda: recorder.status["counters"]["written_total"] == self.count)
            time.sleep(0.25)
            report = recorder.close()
            self.checked_closure(report["status"])
            manifest = report["manifest"]
        else:
            self.event(events, "recording_status", lambda event:
                       event["data"]["counters"]["written_total"] == self.count)
            time.sleep(0.25)
            interrupt_process(process, signal.SIGINT)
            report = self.event(events, "summary")
            self.assertEqual(process.wait(3), 130)
            self.assertTrue(report["resources_closed"])
            self.assertIsNone(report["cleanup_error"])
            self.assertIsNone(report["status"]["error"])
            manifest = report["manifest"]
        self.assertTrue(manifest["clean_close"])
        self.assertTrue(context.ok(), "Bag owns its Node, while the caller owns Context")
        info = aos.inspect_bag(directory)
        self.assertEqual(info["messages"], self.count)
        self.assertEqual({c["topic"] for c in info["channels"]}, {"/secure/allowed", "/secure/denied"})
        return directory, expected, {
            "discovered_channels": 2, "denied_source_control_received": self.count,
            "mcap_business_messages": info["messages"], "resources_closed": True,
            "clean_close": manifest["clean_close"],
        }

    def replay(self, entry, profile, context, directory, target, subscription, expected):
        options = {
            "namespace": "/replay", "topics": ["/secure/allowed"],
            "node": self.node_options(target), "rate": 1000.0,
            # A forbidden publisher may never see matching declarations. The
            # independent authorized control establishes that target's liveness.
            "subscriber_wait_ms": 1000 if target.endswith("/allowed") else 0,
        }
        if entry == "python":
            player = aos.BagPlayer(context, directory, **options)
            self.addCleanup(player.close)
            self.assertEqual(player.status["endpoints"][0]["target"], target)
            player.start()
            report = player.wait(4)
            self.checked_closure(report["status"])
            total = report["status"]["published_total"]
        else:
            options["node"] = {"cli": options["node"]["program"]}
            process, events, _ = self.start_bag("play", options | {
                "directory": str(directory), "security_profile": str(profile),
                "status_interval": 0.02,
            })
            self.event(events, "playback_ready")
            report = self.event(events, "summary")
            self.assertEqual(process.wait(3), 0)
            self.assertTrue(report["resources_closed"])
            self.assertIsNone(report["cleanup_error"])
            self.assertIsNone(report["status"]["error"])
            total = report["status"]["published_total"]
        self.assertEqual(total, self.count)
        if target.endswith("/allowed"):
            actual = [subscription.recv(3)["data"] for _ in expected]
            self.assertEqual(actual, expected)
            received = len(actual)
        else:
            with self.assertRaises(aos.WaitTimeoutError):
                subscription.recv(0.35)
            received = 0
        self.assertTrue(context.ok())
        return {"target": target, "local_published_total": total,
                "actual_received": received, "resources_closed": True}

    def test_installed_record_and_replay_obey_tls_subject_and_remapped_key(self):
        self.record_and_replay(("python", "cli"))

    def test_installed_sdk_record_and_replay_obey_tls_subject_and_remapped_key(self):
        self.record_and_replay(("python",))

    def record_and_replay(self, entries):
        router, profiles = self.start_router()
        policy_digest = aos.compile_security_policy(self.policy)["source_digest"]
        # The CLI and installed native binding must compile identical policy bytes.
        self.assertEqual(self.cli("security", "compile", {"policy": self.policy})["data"]["compiled"]["source_digest"], policy_digest)
        provider_context = self.context(profiles["provider"])
        observer_context = self.context(profiles["provider"])
        restricted_context = self.context(profiles["operator"])
        provider = aos.Node("provider", "/secure", context=provider_context)
        observer = aos.Node("observer", "/secure", context=observer_context)
        self.addCleanup(provider.destroy_node)
        self.addCleanup(observer.destroy_node)
        allowed = provider.create_publisher(self.interface, "/secure/allowed")
        denied = provider.create_publisher(self.interface, "/secure/denied")
        denied_control = observer.create_subscription(self.interface, "/secure/denied")
        denied.wait_for_subscribers(3)
        replay_allowed = observer.create_subscription(self.interface, "/replay/allowed")
        replay_denied = observer.create_subscription(self.interface, "/replay/denied")
        replay_control = provider.create_publisher(self.interface, "/replay/denied")
        replay_control.wait_for_subscribers(3)
        cases = []
        for entry in entries:
            with self.subTest(entry=entry):
                directory, expected, recorded = self.record(
                    entry, profiles["operator"], restricted_context, allowed, denied, denied_control,
                )
                before = f"{entry}-authorized-before"
                replay_control.publish({"data": before})
                self.assertEqual(replay_denied.recv(3), {"data": before})
                reports = []
                for target, subscription in (("/replay/allowed", replay_allowed),
                                              ("/replay/denied", replay_denied)):
                    reports.append(self.replay(entry, profiles["operator"], restricted_context,
                                               directory, target, subscription, expected))
                after = f"{entry}-authorized-after"
                replay_control.publish({"data": after})
                self.assertEqual(replay_denied.recv(3), {"data": after})
                # inspect counted exactly eight business messages; replay of only
                # the allowed source delivered those same eight payloads. Thus no
                # denied-source business payload was hidden in the recorded bag.
                recorded["allowed_source_replay_verified"] = expected
                recorded["denied_source_recorded"] = 0
                cases.append({"entry": entry, "record": recorded, "replay": reports,
                              "same_target_controls": [before, after]})
        self.assertEqual(len(cases), len(entries), "every selected entry point must complete")
        interrupt_process(router, signal.SIGTERM)
        stdout, stderr = router.communicate(timeout=4)
        self.assertEqual(router.returncode, 130 if os.name == "nt" else 143, stdout + stderr)
        shutdown = [json.loads(line) for line in stdout.splitlines() if line.strip()]
        summaries = [event["data"] for event in shutdown if event["event"] == "summary"]
        self.assertEqual(len(summaries), 1, shutdown)
        self.assertTrue(summaries[0]["session_closed"], shutdown)
        self.assertIsNone(summaries[0]["cleanup_error"], shutdown)
        result = {"status": "passed", "router": "installed aos security serve",
                  "router_pid": router.pid, "router_session_closed": True,
                  "policy_digest": policy_digest, "domain_id": 29,
                  "denied_replay_observation_ms": 350, "cases": cases,
                  "acl_trace": "separate same-source Rust secure_bag integration test"}
        if self.result_path:
            destination = Path(self.result_path)
            if entries == ("python",):
                destination = destination.with_stem(destination.stem + "-sdk")
            destination.write_text(json.dumps(result, indent=2) + "\n")
        print("secure Bag installed evidence: " + json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    unittest.main()
