"""Parameter contracts exercised through the installed wheel and real Zenoh."""

import json
import threading
import time
import unittest

import aos_sdk as aos

from test_platform_foundation import PlatformTestCase


def parameter(name, value):
    return aos.Parameter(name, value=value)


class ParameterValueTest(unittest.TestCase):
    def test_native_type_inference_and_explicit_empty_arrays(self):
        cases = [
            (None, aos.ParameterType.NOT_SET), (True, aos.ParameterType.BOOL),
            (7, aos.ParameterType.INTEGER), (7.0, aos.ParameterType.DOUBLE),
            ("hello", aos.ParameterType.STRING), ([True], aos.ParameterType.BOOL_ARRAY),
            ([1, 2], aos.ParameterType.INTEGER_ARRAY),
            ([1.0, 2.0], aos.ParameterType.DOUBLE_ARRAY),
            (["one", "two"], aos.ParameterType.STRING_ARRAY),
            (bytes([0, 255]), aos.ParameterType.BYTE_ARRAY),
        ]
        for value, expected in cases:
            with self.subTest(value=value):
                actual = parameter("value", value)
                self.assertEqual(actual.type_, expected)
                self.assertEqual(actual.get_parameter_value()["type"], expected.value)
        empty = aos.Parameter("empty", aos.ParameterType.INTEGER_ARRAY, [])
        self.assertEqual(empty.value, [])
        copy = parameter("copy", [1, 2])
        copy.value.append(3)
        self.assertEqual(copy.value, [1, 2])
        for value in ([], [1, True], [1, 1.0], [[1]], {}, 2**63, float("nan"), float("inf")):
            with self.subTest(value=value), self.assertRaises(aos.AOSError):
                parameter("bad", value)
        for value in (
            {"type": "integer", "value": True},
            {"type": "integer", "value": 1, "unknown": 2},
            {"type": "not_set", "value": 2},
            {"type": "byte_array", "value": [-1]},
            {"type": "byte_array", "value": [256]},
        ):
            with self.subTest(value=value), self.assertRaises(aos.ParameterError):
                parameter("bad", value)


class ParameterTest(PlatformTestCase):
    def events(self, name="observer"):
        observer = self.node(name, start_parameter_services=False)
        return observer.create_subscription(
            aos.parameter_event_interface(), "/parameter_events",
            qos_profile="parameter_events",
        )

    def event(self, subscription, node, version):
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            event = subscription.recv(max(0, deadline - time.monotonic()))
            if event["node"] == node.fully_qualified_name and event["version"] == version:
                return event
        self.fail("expected parameter event was not received")

    def remote_client(self, observer, provider, kind):
        client = observer.create_client(
            aos.parameter_service_interface(kind),
            f"{provider.fully_qualified_name}/{kind}", qos_profile="parameters",
        )
        self.assertTrue(client.wait_for_service(2))
        return client

    def test_declaration_startup_layers_sources_and_strict_descriptors(self):
        first = self.config("parameters-first.json", {
            "parameters": {"rate": 2, "file_only": "first"},
            "nodes": {"worker": {"parameters": {"rate": 3}}},
        })
        second = self.config("parameters-second.json", {"parameters": {"rate": 4}})
        node = self.node(
            "worker", config_files=[first, second],
            parameter_overrides=[parameter("rate", 1), parameter("program_only", True)],
            launch_config={"parameters": {"rate": 5, "launch_only": [1, 2]}},
            cli_overrides={"parameters": {"rate": 6}},
        )
        self.assertFalse(node.has_parameter("rate"))
        with self.assertRaises(aos.ParameterError) as missing:
            node.get_parameter("rate")
        self.assertEqual(missing.exception.code, "not_declared")
        descriptor = aos.ParameterDescriptor(
            description="update rate", integer_range=aos.IntegerRange(0, 10, 2),
        )
        self.assertEqual(node.declare_parameter("rate", 0, descriptor).value, 6)
        node.declare_parameter("file_only", "")
        node.declare_parameter("program_only", False)
        node.declare_parameter("launch_only", {"type": "integer_array", "value": []})
        records = {p["name"]: p for p in node.get_parameter_snapshot()["parameters"]}
        self.assertEqual(records["rate"]["source"], "cli")
        self.assertEqual(records["rate"]["default_value"], {"type": "integer", "value": 0})
        self.assertEqual(records["file_only"]["source"], f"file:{first}")
        self.assertEqual(records["program_only"]["source"], "program")
        self.assertEqual(records["launch_only"]["source"], "launch")
        self.assertEqual(node.describe_parameter("rate").description, "update rate")
        self.assertEqual(node.describe_parameter("rate").integer_range.step, 2)
        self.assertEqual(node.get_parameter_types(["rate"]), [aos.ParameterType.INTEGER])
        with self.assertRaises(aos.ParameterError) as duplicate:
            node.declare_parameter("rate", 0)
        self.assertEqual(duplicate.exception.code, "already_declared")
        for descriptor in ({"unknown": True}, {"read_only": 1}, {"integer_range": {
            "from_value": 0, "to_value": 10, "step": True,
        }}):
            with self.subTest(descriptor=descriptor), self.assertRaises(aos.ParameterError):
                node.declare_parameter("bad", 1, descriptor)
        bad = self.node("bad_override", parameter_overrides={"value": "wrong type"})
        with self.assertRaises(aos.ParameterError) as wrong:
            bad.declare_parameter("value", 0)
        self.assertEqual(wrong.exception.code, "wrong_type")
        self.assertEqual(bad.declare_parameter("value", 0, ignore_override=True).value, 0)

    def test_uninitialized_static_dynamic_readonly_and_namespace_listing(self):
        node = self.node("descriptors")
        declared = node.declare_parameter("static", aos.ParameterType.INTEGER)
        self.assertIsNone(declared.value)
        with self.assertRaises(aos.ParameterError) as uninitialized:
            node.get_parameter("static")
        self.assertEqual(uninitialized.exception.code, "uninitialized")
        self.assertEqual(node.get_parameter_types(["static"]), [aos.ParameterType.NOT_SET])
        self.assertEqual(
            node.describe_parameter("static").parameter_type, aos.ParameterType.INTEGER,
        )
        self.assertTrue(node.set_parameters_atomically([parameter("static", 3)]).successful)
        self.assertEqual(node.get_parameter_types(["static"]), [aos.ParameterType.INTEGER])
        self.assertEqual(
            node.set_parameters_atomically([parameter("static", True)]).code, "wrong_type",
        )
        node.declare_parameter("locked", 1, aos.ParameterDescriptor(read_only=True))
        self.assertEqual(node.set_parameters_atomically([parameter("locked", 2)]).code, "read_only")
        node.declare_parameter("dynamic", 1, aos.ParameterDescriptor(dynamic_typing=True))
        self.assertTrue(node.set_parameters_atomically([parameter("dynamic", "changed")]).successful)
        self.assertEqual(node.get_parameter_types(["dynamic"]), [aos.ParameterType.STRING])
        self.assertTrue(node.set_parameters_atomically([parameter("dynamic", None)]).successful)
        self.assertFalse(node.has_parameter("dynamic"))
        node.declare_parameter("camera.width", 640)
        node.declare_parameter("camera.exposure.mode", "auto")
        listing = node.list_parameters(["camera"], depth=1)
        self.assertEqual(listing.names, ["camera.width"])
        self.assertEqual(node.list_parameters(["camera"]).names, [
            "camera.exposure.mode", "camera.width",
        ])
        with self.assertRaises(aos.InvalidArgumentError):
            node.list_parameters(depth=True)

    def test_atomic_failure_has_no_partial_write_version_or_event(self):
        node = self.node("atomic")
        node.declare_parameter("first", 1)
        node.declare_parameter("second", 2, aos.ParameterDescriptor(
            integer_range=aos.IntegerRange(0, 10, 2),
        ))
        events = self.events()
        before = node.get_parameter_snapshot()
        calls = []
        node.add_on_set_parameters_callback(
            lambda candidate: calls.append(candidate) or aos.SetParametersResult(True),
        )
        result = node.set_parameters_atomically([parameter("first", 5), parameter("second", 3)])
        self.assertFalse(result.successful)
        self.assertEqual(result.code, "out_of_range")
        self.assertEqual(node.get_parameter_snapshot(), before)
        self.assertEqual(calls, [])
        with self.assertRaises(aos.WaitTimeoutError):
            events.recv(0.05)
        result = node.set_parameters_atomically([parameter("first", 5), parameter("second", 4)])
        self.assertTrue(result.successful)
        self.assertTrue(result.event_published)
        self.assertEqual(result.version, before["version"] + 1)
        self.assertEqual(len(calls), 1)
        event = self.event(events, node, result.version)
        self.assertEqual(event["transaction_id"], result.transaction_id)
        self.assertEqual([p["name"] for p in event["changed_parameters"]], ["first", "second"])
        self.assertEqual([p.value for p in node.get_parameters(["first", "second"])], [5, 4])
        results = node.set_parameters([parameter("first", 6), parameter("second", 3)])
        self.assertEqual([result.successful for result in results], [True, False])
        self.assertEqual([p.value for p in node.get_parameters(["first", "second"])], [6, 4])

    def test_validators_run_outside_store_lock_and_fail_closed(self):
        node = self.node("validators")
        node.declare_parameter("value", 1)
        calls = []

        def original(candidate):
            calls.append(("original", threading.get_ident(), candidate[0].value))
            self.assertEqual(node.get_parameter("value").value, 1)
            return aos.SetParametersResult(True)

        def newest(candidate):
            calls.append(("newest", threading.get_ident(), candidate[0].value))
            return aos.SetParametersResult(False, "not allowed")

        node.add_on_set_parameters_callback(original)
        node.add_on_set_parameters_callback(newest)
        rejected = node.set_parameters_atomically([parameter("value", 2)])
        self.assertEqual((rejected.code, rejected.reason), ("validation", "not allowed"))
        self.assertEqual(calls, [("newest", threading.get_ident(), 2)])
        node.remove_on_set_parameters_callback(newest)
        self.assertTrue(node.set_parameters_atomically([parameter("value", 3)]).successful)
        self.assertEqual(calls[-1], ("original", threading.get_ident(), 3))
        node.remove_on_set_parameters_callback(original)

        def raises(_):
            raise RuntimeError("private diagnostic must stay local")

        node.add_on_set_parameters_callback(raises)
        with self.assertLogs("aos_sdk", level="ERROR"):
            result = node.set_parameters_atomically([parameter("value", 4)])
        self.assertFalse(result.successful)
        self.assertEqual(result.code, "validation")
        self.assertNotIn("private diagnostic", result.reason)
        self.assertEqual(node.get_parameter("value").value, 3)
        node.remove_on_set_parameters_callback(raises)
        node.add_on_set_parameters_callback(lambda _: True)
        with self.assertLogs("aos_sdk", level="ERROR"):
            result = node.set_parameters_atomically([parameter("value", 5)])
        self.assertEqual(result.code, "validation")

    def test_conflict_and_expiry_do_not_retry_validator_or_overwrite(self):
        node = self.node("transactions")
        node.declare_parameter("first", 0)
        node.declare_parameter("second", 0)
        entered = threading.Event()
        release = threading.Event()
        calls = []

        def validate(candidate):
            calls.append(candidate[0].name)
            if candidate[0].name == "first":
                entered.set()
                self.assertTrue(release.wait(2))
            return aos.SetParametersResult(True)

        node.add_on_set_parameters_callback(validate)
        outcome = []
        worker = threading.Thread(
            target=lambda: outcome.append(node.set_parameters_atomically([parameter("first", 1)])),
        )
        worker.start()
        self.assertTrue(entered.wait(2))
        self.assertTrue(node.set_parameters_atomically([parameter("second", 2)]).successful)
        release.set()
        worker.join(2)
        self.assertFalse(worker.is_alive())
        self.assertEqual(outcome[0].code, "conflict")
        self.assertEqual(calls, ["first", "second"])
        self.assertEqual([p.value for p in node.get_parameters(["first", "second"])], [0, 2])
        node.remove_on_set_parameters_callback(validate)
        node.add_on_set_parameters_callback(
            lambda _: time.sleep(0.03) or aos.SetParametersResult(True),
        )
        version = node.get_parameter_snapshot()["version"]
        expired = node.set_parameters_atomically([parameter("first", 3)], timeout_sec=0.01)
        self.assertEqual(expired.code, "expired")
        self.assertEqual(node.get_parameter_snapshot()["version"], version)
        for timeout in (True, -1, float("nan"), float("inf")):
            with self.subTest(timeout=timeout), self.assertRaises(aos.InvalidArgumentError):
                node.set_parameters_atomically([parameter("first", 3)], timeout_sec=timeout)

    def test_six_real_services_share_executor_validator_and_store(self):
        node = self.node("provider", "/params", cli_overrides={"name": "renamed"})
        observer = self.node("client", start_parameter_services=False)
        node.declare_parameter("value", 10)
        node.declare_parameter("locked", 2, aos.ParameterDescriptor(read_only=True))
        node.declare_parameter("typed_empty", aos.ParameterType.INTEGER)
        calls = []

        def validate(candidate):
            calls.append((threading.current_thread().name, candidate[0].value))
            self.assertEqual(node.get_parameter("value").value, 10)
            return aos.SetParametersResult(candidate[0].value <= 20, "too large")

        node.add_on_set_parameters_callback(validate)
        clients = {
            kind: self.remote_client(observer, node, kind) for kind in (
                "describe_parameters", "get_parameter_types", "get_parameters",
                "list_parameters", "set_parameters", "set_parameters_atomically",
            )
        }
        # Discovery is independent of Executor dispatch. No request is sent
        # until the executor is running, avoiding a timed-out side effect.
        self.spin(node)
        version = node.get_parameter_snapshot()["version"]
        query = clients["get_parameters"].call({"names": ["value"]}, 2)
        self.assertEqual(query, {
            "version": version, "value": [{"type": "integer", "value": 10}], "error": None,
        })
        types = clients["get_parameter_types"].call({"names": ["value", "typed_empty"]}, 2)
        self.assertEqual(types["value"], ["integer", "not_set"])
        records = clients["describe_parameters"].call({"names": ["value", "locked"]}, 2)
        self.assertEqual(records["value"][0]["default_value"]["value"], 10)
        self.assertTrue(records["value"][1]["descriptor"]["read_only"])
        listed = clients["list_parameters"].call({"prefixes": [], "depth": 0}, 2)
        self.assertIn("value", listed["value"]["names"])
        failed = clients["get_parameters"].call({"names": ["missing"]}, 2)
        self.assertEqual(failed["error"]["code"], "not_declared")
        atomic = clients["set_parameters_atomically"].call({
            "parameters": [parameter("value", 30).to_dict()],
        }, 2)["result"]
        self.assertFalse(atomic["successful"])
        self.assertEqual(atomic["code"], "validation")
        result = clients["set_parameters"].call({
            "parameters": [parameter("value", 15).to_dict(), parameter("locked", 3).to_dict()],
        }, 2)["results"]
        self.assertEqual([item["successful"] for item in result], [True, False])
        self.assertEqual(result[1]["code"], "read_only")
        self.assertEqual(node.get_parameter("value").value, 15)
        self.assertEqual([value for _, value in calls], [30, 15])
        self.assertTrue(all(name.startswith("aos-callback") for name, _ in calls), calls)
        entries = {
            entry["name"]: entry for entry in node.get_node_card()["endpoints"]
            if entry["name"].startswith(node.fully_qualified_name + "/")
        }
        for kind in clients:
            entry = entries[f"{node.fully_qualified_name}/{kind}"]
            self.assertEqual(entry["qos"]["preset"], "parameters")
            self.assertEqual(entry["qos"]["depth"], 1000)
        self.assertFalse(any(
            entry["name"].endswith("/get_parameters")
            for entry in observer.get_node_card()["endpoints"]
            if entry["kind"] == "service"
        ))

    def test_dump_load_is_explicit_atomic_and_validated(self):
        node = self.node("persistence")
        node.declare_parameter("value", 1)
        path = self.directory / "parameters.json"
        self.assertFalse(path.exists())
        node.dump_parameters(path)
        saved = json.loads(path.read_text())
        self.assertEqual(saved["node"], node.fully_qualified_name)
        self.assertTrue(node.set_parameters_atomically([parameter("value", 2)]).successful)
        self.assertEqual(json.loads(path.read_text()), saved)
        self.assertTrue(node.load_parameters(path).successful)
        self.assertEqual(node.get_parameter("value").value, 1)
        saved["parameters"].append({
            **next(record for record in saved["parameters"] if record["name"] == "value"),
            "name": "missing",
        })
        path.write_text(json.dumps(saved))
        before = node.get_parameter_snapshot()
        failed = node.load_parameters(path)
        self.assertEqual(failed.code, "not_declared")
        self.assertEqual(node.get_parameter_snapshot(), before)
        path.write_text('{"format_version":1,"format_version":2}')
        self.assertEqual(node.load_parameters(path).code, "invalid_argument")

    def test_parameter_services_close_with_node_and_context(self):
        node = self.node("closing")
        observer = self.node("closing_client", start_parameter_services=False)
        client = self.remote_client(observer, node, "get_parameters")
        node.declare_parameter("value", 1)
        node.destroy_node()
        self.assertFalse(client.wait_for_service(0.2))
        with self.assertRaises(aos.ParameterError) as error:
            node.get_parameter("value")
        self.assertEqual(error.exception.code, "closed")
        other = self.node("context_closing")
        self.context.shutdown()
        with self.assertRaises(aos.ParameterError) as error:
            other.get_parameter_snapshot()
        self.assertEqual(error.exception.code, "closed")

    def test_disabling_parameter_services_preserves_local_events(self):
        events = self.events()
        node = self.node("local_parameters", start_parameter_services=False)
        node.declare_parameter("value", 1)
        result = node.set_parameters_atomically([parameter("value", 2)])
        self.assertTrue(result.successful)
        self.assertTrue(result.event_published)
        event = self.event(events, node, result.version)
        self.assertEqual(event["changed_parameters"][0]["value"]["value"], 2)
        endpoints = node.get_node_card()["endpoints"]
        self.assertTrue(any(
            entry["name"] == "/parameter_events" and entry["kind"] == "publisher"
            for entry in endpoints
        ))
        self.assertFalse(any(
            entry["name"].endswith("/get_parameters") and entry["kind"] == "service"
            for entry in endpoints
        ))

    def test_shutdown_during_validation_rejects_pending_commit(self):
        node = self.node("closing_transaction")
        node.declare_parameter("value", 1)
        entered = threading.Event()
        release = threading.Event()
        outcome = []

        def validate(_):
            entered.set()
            self.assertTrue(release.wait(2))
            return aos.SetParametersResult(True)

        node.add_on_set_parameters_callback(validate)
        worker = threading.Thread(
            target=lambda: outcome.append(node.set_parameters_atomically([parameter("value", 2)])),
        )
        worker.start()
        self.assertTrue(entered.wait(2))
        start = time.monotonic()
        try:
            self.context.shutdown()
        finally:
            release.set()
            worker.join(2)
        self.assertLess(time.monotonic() - start, 1)
        self.assertFalse(worker.is_alive())
        self.assertEqual(outcome[0].code, "closed")

    def test_use_sim_time_parameter_and_clock_share_validation_and_state(self):
        node = self.node("clock_parameter")
        self.assertFalse(node.get_parameter("use_sim_time").value)
        self.assertFalse(node.clock.now().use_sim_time)
        reject = lambda _: aos.SetParametersResult(False, "simulation disabled")
        node.add_on_set_parameters_callback(reject)
        with self.assertRaises(aos.ParameterError) as error:
            node.clock.set_use_sim_time(True)
        self.assertEqual(error.exception.code, "validation")
        self.assertFalse(node.clock.now().use_sim_time)
        self.assertFalse(node.get_parameter("use_sim_time").value)
        node.remove_on_set_parameters_callback(reject)
        self.assertTrue(
            node.set_parameters_atomically([parameter("use_sim_time", True)]).successful,
        )
        self.assertTrue(node.clock.now().use_sim_time)
        self.assertFalse(node.clock.now().initialized)
        source = self.node("clock_parameter_source", start_parameter_services=False)
        publisher = source.create_publisher(aos.clock_interface(), "/clock", "clock")
        publisher.wait_for_subscribers(2)
        publisher.publish({"clock": {"sec": 2, "nanosec": 0}})
        self.wait_for(lambda: node.clock.now().nanoseconds == 2_000_000_000)
        node.set_use_sim_time(False)
        self.assertFalse(node.get_parameter("use_sim_time").value)
        self.assertFalse(node.clock.now().use_sim_time)
        self.assertTrue(node.clock.now().initialized)


if __name__ == "__main__":
    unittest.main()
