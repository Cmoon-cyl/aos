"""Parameter snapshot restoration through an installed native wheel."""

import json

import aos_sdk as aos

from test_platform_foundation import PlatformTestCase


class ParameterRestoreTest(PlatformTestCase):
    def declared(self):
        node = self.node("restore")
        node.declare_parameter(
            "dynamic", None, aos.ParameterDescriptor(dynamic_typing=True),
        )
        node.declare_parameter("static", aos.ParameterType.INTEGER)
        node.declare_parameter("constant", 3, aos.ParameterDescriptor(read_only=True))
        node.declare_parameter("changed", 1)
        return node

    def event_subscription(self):
        observer = self.node("observer")
        subscription = observer.create_subscription(
            aos.parameter_event_interface(), "/parameter_events",
            qos_profile="parameter_events",
        )
        while True:
            try:
                subscription.recv(0.0)
            except aos.WaitTimeoutError:
                return observer, subscription

    def test_full_snapshot_no_op_preserves_uninitialized_read_only_and_versions(self):
        node = self.declared()
        _, events = self.event_subscription()
        before = node.get_parameter_snapshot()
        path = self.directory / "snapshot.json"
        node.dump_parameters(path)
        observed = []

        def validate(parameters):
            observed.append({p.name: p.value for p in parameters})
            return aos.SetParametersResult(successful=True)

        node.add_on_set_parameters_callback(validate)
        result = node.load_parameters(path)
        self.assertTrue(result.successful, result.reason)
        self.assertEqual(result.version, before["version"])
        self.assertFalse(result.event_published)
        self.assertEqual(node.get_parameter_snapshot(), before)
        self.assertEqual(len(observed), 1)
        self.assertEqual(set(observed[0]), {
            "use_sim_time", "dynamic", "static", "constant", "changed",
        })
        self.assertIsNone(observed[0]["dynamic"])
        self.assertIsNone(observed[0]["static"])
        self.assertTrue(node.has_parameter("dynamic"))
        self.assertTrue(node.has_parameter("static"))
        with self.assertRaises(aos.WaitTimeoutError):
            events.recv(0.05)

    def test_mixed_load_emits_one_event_for_only_the_changed_value(self):
        node = self.declared()
        path = self.directory / "snapshot.json"
        node.dump_parameters(path)
        node.set_parameters_atomically([aos.Parameter("changed", value=4)])
        _, events = self.event_subscription()
        before = node.get_parameter_snapshot()
        result = node.load_parameters(path)
        self.assertTrue(result.successful, result.reason)
        self.assertEqual(result.version, before["version"] + 1)
        self.assertTrue(result.event_published)
        self.assertEqual(node.get_parameter("changed").value, 1)
        event = events.recv(2)
        self.assertEqual(event["changed_parameters"], [{
            "name": "changed", "value": {"type": "integer", "value": 1},
        }])
        self.assertEqual(event["new_parameters"], [])
        self.assertEqual(event["deleted_parameters"], [])
        records = {record["name"]: record for record in node.get_parameter_snapshot()["parameters"]}
        for record in before["parameters"]:
            if record["name"] != "changed":
                self.assertEqual(records[record["name"]], record)
        with self.assertRaises(aos.WaitTimeoutError):
            events.recv(0.05)

    def test_snapshot_conflicts_and_validator_rejection_leave_the_batch_unchanged(self):
        node = self.declared()
        path = self.directory / "snapshot.json"
        node.dump_parameters(path)
        snapshot = json.loads(path.read_text())
        node.set_parameters_atomically([
            aos.Parameter("dynamic", value=7), aos.Parameter("changed", value=2),
        ])
        before = node.get_parameter_snapshot()
        result = node.load_parameters(path)
        self.assertEqual(result.code, "conflict")
        self.assertEqual(node.get_parameter_snapshot(), before)
        for record in snapshot["parameters"]:
            if record["name"] == "dynamic":
                record["value"] = {"type": "integer", "value": 7}
            if record["name"] == "constant":
                record["value"] = {"type": "integer", "value": 9}
        path.write_text(json.dumps(snapshot))
        self.assertEqual(node.load_parameters(path).code, "read_only")
        self.assertEqual(node.get_parameter_snapshot(), before)

        node.dump_parameters(path)
        seen = []

        def reject(parameters):
            seen.append([p.name for p in parameters])
            return aos.SetParametersResult(successful=False, reason="reject unchanged load")

        node.add_on_set_parameters_callback(reject)
        self.assertEqual(node.load_parameters(path).code, "validation")
        self.assertEqual(len(seen[0]), len(before["parameters"]))
        self.assertEqual(node.get_parameter_snapshot(), before)

    def test_remote_atomic_load_keeps_all_candidates_and_default_set_still_deletes(self):
        node = self.declared()
        observer, events = self.event_subscription()
        client = observer.create_client(
            aos.parameter_service_interface("set_parameters_atomically"),
            f"{node.fully_qualified_name}/set_parameters_atomically",
            qos_profile="parameters",
        )
        self.assertTrue(client.wait_for_service(2))
        before = node.get_parameter_snapshot()
        candidates = [{"name": p["name"], "value": p["value"]} for p in before["parameters"]]
        observed = []

        def validate(parameters):
            observed.append([p.name for p in parameters])
            return aos.SetParametersResult(successful=True)

        node.add_on_set_parameters_callback(validate)
        self.spin(node)
        reply = client.call({"parameters": candidates, "mode": "load"}, timeout=2)["result"]
        self.assertTrue(reply["successful"], reply)
        self.assertEqual(reply["version"], before["version"])
        self.assertFalse(reply["event_published"])
        self.assertEqual(observed, [[p["name"] for p in before["parameters"]]])
        self.assertEqual(node.get_parameter_snapshot(), before)
        with self.assertRaises(aos.WaitTimeoutError):
            events.recv(0.05)
        reply = client.call({
            "parameters": [{"name": "dynamic", "value": {"type": "not_set"}}],
        }, timeout=2)["result"]
        self.assertTrue(reply["successful"], reply)
        self.assertFalse(node.has_parameter("dynamic"))
        self.assertEqual(events.recv(2)["deleted_parameters"], [{
            "name": "dynamic", "value": {"type": "not_set"},
        }])
