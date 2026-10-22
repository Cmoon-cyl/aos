"""Management CLI acceptance against the installed wheel and a real router."""

from process_support import console_process_options, interrupt_process, read_line

import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import threading
import time
import unittest
from unittest import mock
import uuid

import aos_sdk as aos
from aos_sdk.parameter import _ParameterServices
from test_stage1_cli import RouterProcess


class ManagementCliTest(unittest.TestCase):
    def setUp(self):
        self.binary = os.environ["AOS_CLI"]
        self.assertTrue(Path(self.binary).is_file())
        temporary = tempfile.TemporaryDirectory(prefix="aos-management-cli-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.environment = dict(os.environ)
        for key in ("AOS_NODE_NAME", "AOS_NAMESPACE", "AOS_USE_SIM_TIME",
                    "AOS_ZENOH_CONFIG", "AOS_DOMAIN_ID", "AOS_INTERFACE_PATH"):
            self.environment.pop(key, None)

    def start(self, group, verb, request, *, environment=None):
        process = subprocess.Popen(
            [self.binary, group, verb, "--request", "-"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, encoding="utf-8", env=self.environment | (environment or {}), **console_process_options(),
        )
        process.stdin.write(json.dumps(request))
        process.stdin.close()
        process.stdin = None

        def stop():
            if process.poll() is None:
                process.terminate()
                process.communicate(timeout=5)

        self.addCleanup(stop)
        return process

    def finish(self, process, exit_code=0):
        stdout, stderr = process.communicate(timeout=12)
        self.assertEqual(process.returncode, exit_code, stdout + stderr)
        lines = stdout.splitlines()
        self.assertEqual(len(lines), 1, stdout + stderr)
        value = json.loads(lines[0])
        self.assertEqual(value["ok"], exit_code == 0, value)
        return value["data" if exit_code == 0 else "error"]

    def cli(self, group, verb, request, *, exit_code=0, environment=None):
        return self.finish(self.start(group, verb, request, environment=environment), exit_code)

    def peers(self, node_type=aos.Node, **options):
        router = RouterProcess()
        self.addCleanup(router.close)
        patch = mock.patch.dict(os.environ, self.environment, clear=True)
        patch.start()
        self.addCleanup(patch.stop)
        self.context = aos.Context(config=router.config, domain_id=93)
        self.addCleanup(self.context.shutdown)
        self.namespace = "/management_" + uuid.uuid4().hex
        node = node_type("provider", self.namespace, self.context, **options)
        self.addCleanup(node.destroy_node)
        self.environment.update(AOS_ZENOH_CONFIG=str(router.config), AOS_DOMAIN_ID="93")
        return node

    def spin(self, *nodes):
        executor = aos.MultiThreadedExecutor(2, context=self.context)
        for node in nodes:
            executor.add_node(node)
        thread = threading.Thread(target=executor.spin, name="management-cli-provider")
        thread.start()

        def stop():
            self.assertTrue(executor.shutdown(timeout_sec=5))
            thread.join(5)
            self.assertFalse(thread.is_alive())

        self.addCleanup(stop)
        return executor

    def request(self, node, **fields):
        return {"node_name": node.fully_qualified_name, "timeout": 3, **fields}

    def test_preflight_rejects_values_and_paths_before_opening_context(self):
        bad_network = {"AOS_ZENOH_CONFIG": str(self.directory / "not-a-session.json5")}
        for parameters in ({"empty": []}, {"mixed": [1, True]},
                           {"integer": {"type": "integer", "value": True}}):
            with self.subTest(parameters=parameters):
                error = self.cli("param", "set", {"node_name": "/provider", "parameters": parameters},
                                 exit_code=2, environment=bad_network)
                self.assertEqual(error["code"], "INVALID_ARGUMENT")
                self.assertNotIn("session configuration", error["message"])
        for group, verb, fields in (
            ("param", "load", {"path": str(self.directory / "missing.json")}),
            ("param", "dump", {"path": str(self.directory / "missing-parent" / "dump.json")}),
        ):
            error = self.cli(group, verb, {"node_name": "/provider", **fields},
                             exit_code=1, environment=bad_network)
            self.assertEqual(error["code"], "FILE_IO_ERROR")
            self.assertNotIn("session configuration", error["message"])
        duplicate = self.directory / "duplicate.json"
        duplicate.write_text('{"parameters":{"x":1,"x":2}}', encoding="utf-8")
        error = self.cli("param", "load", {"node_name": "/provider", "path": str(duplicate)},
                         exit_code=2, environment=bad_network)
        self.assertIn("duplicate", error["message"].lower())
        for transition in (0, 8, 3.0, "transition_success"):
            error = self.cli("lifecycle", "set", {"node_name": "/provider", "transition": transition},
                             exit_code=2, environment=bad_network)
            self.assertEqual(error["code"], "INVALID_ARGUMENT")
            self.assertNotIn("session configuration", error["message"])
        error = self.cli("param", "list", {"node_name": "/provider", "depth": 1.0},
                         exit_code=2, environment=bad_network)
        self.assertEqual(error["code"], "INVALID_ARGUMENT")
        self.assertNotIn("session configuration", error["message"])

    def test_parameter_queries_and_one_atomic_batch_keep_typed_empty_arrays(self):
        node = self.peers()
        node.declare_parameter("speed", 1, aos.ParameterDescriptor(
            description="speed limit", integer_range=aos.IntegerRange(0, 10)))
        node.declare_parameter("items", {"type": "integer_array", "value": [1]})
        node.declare_parameter("camera.width", 640)
        node.declare_parameter("camera.exposure.mode", "auto")
        validated = []
        node.add_on_set_parameters_callback(
            lambda candidate: (validated.append([p.name for p in candidate])
                               or aos.SetParametersResult(True)))
        self.spin(node)
        before = node.get_parameter_snapshot()["version"]
        rejected = self.cli("param", "set", self.request(
            node, parameters={"items": [9], "speed": 99}), exit_code=1)
        self.assertEqual(rejected["code"], "PARAMETER_OUT_OF_RANGE")
        self.assertFalse(rejected["parameter_result"]["successful"])
        self.assertEqual(node.get_parameter("items").value, [1])
        self.assertEqual(node.get_parameter_snapshot()["version"], before)
        self.assertEqual(validated, [])
        result = self.cli("param", "set", self.request(
            node, parameters={"items": {"type": "integer_array", "value": []}, "speed": 3}))
        self.assertTrue(result["atomic"])
        self.assertTrue(result["result"]["successful"])
        self.assertEqual(result["result"]["version"], before + 1)
        self.assertEqual(validated, [["items", "speed"]])
        values = self.cli("param", "get", self.request(node, names=["speed", "items"]))
        self.assertEqual(values["parameters"], [
            {"name": "speed", "value": {"type": "integer", "value": 3}},
            {"name": "items", "value": {"type": "integer_array", "value": []}},
        ])
        described = self.cli("param", "describe", self.request(node, names=["speed"]))
        self.assertEqual(described["parameters"][0]["descriptor"]["description"], "speed limit")
        listing = self.cli("param", "list", self.request(node, prefixes=["camera"], depth=1))
        self.assertEqual(listing["names"], ["camera.width"])
        self.assertFalse(listing["retry"])

    def test_dump_load_and_startup_restore_share_the_core_values(self):
        node = self.peers()
        node.declare_parameter("speed", 3)
        node.declare_parameter("items", {"type": "integer_array", "value": []})
        self.spin(node)
        startup = self.directory / "startup.json"
        dumped = self.cli("param", "dump", self.request(node, path=str(startup)))
        document = json.loads(startup.read_text())
        self.assertEqual(document, dumped["document"])
        self.assertEqual(document["parameters"]["items"], {"type": "integer_array", "value": []})
        self.assertTrue(dumped["consistent_versions"])
        self.assertIsInstance(dumped["file_write"]["directory_synced"], bool)
        self.cli("param", "set", self.request(node, parameters={"speed": 7, "items": [1, 2]}))
        loaded = self.cli("param", "load", self.request(node, path=str(startup)))
        self.assertTrue(loaded["atomic"])
        self.assertEqual(node.get_parameter("speed").value, 3)
        self.assertEqual(node.get_parameter("items").value, [])

        restored = aos.Node("restored", self.namespace, self.context, config_files=[startup])
        self.addCleanup(restored.destroy_node)
        self.assertFalse(restored.has_parameter("speed"))
        self.assertEqual(restored.declare_parameter("speed", 0).value, 3)
        self.assertEqual(restored.declare_parameter(
            "items", {"type": "integer_array", "value": [99]}).value, [])

        snapshot = self.directory / "snapshot.json"
        dumped = self.cli("param", "dump", self.request(node, path=str(snapshot), format="snapshot"))
        self.assertEqual(dumped["document"]["format_version"], 1)
        self.assertEqual(dumped["document"]["node"], node.fully_qualified_name)
        self.cli("param", "set", self.request(node, parameters={"speed": 9}))
        self.cli("param", "load", self.request(node, path=str(snapshot)))
        self.assertEqual(node.get_parameter("speed").value, 3)

    def test_load_keeps_the_preflight_values_when_the_file_changes_during_discovery(self):
        self.peers()
        path = self.directory / "load-once.json"
        path.write_text('{"parameters":{"value":2}}', encoding="utf-8")
        target = self.namespace + "/late_provider"
        process = self.start("param", "load", {
            "node_name": target, "path": str(path), "timeout": 5,
        })
        deadline = time.monotonic() + 3
        observed_cli = False
        while time.monotonic() < deadline:
            if any(entity.get("node_name", "").startswith("aos_cli_")
                   for entity in self.context.graph(0.1)):
                observed_cli = True
                break
        self.assertTrue(observed_cli, "CLI did not reach discovery after preparing the file")
        path.write_text("invalid replacement that must not be read again", encoding="utf-8")
        node = aos.Node("late_provider", self.namespace, self.context)
        self.addCleanup(node.destroy_node)
        node.declare_parameter("value", 1)
        validated = []
        node.add_on_set_parameters_callback(
            lambda candidate: (validated.append([p.value for p in candidate])
                               or aos.SetParametersResult(True)))
        self.spin(node)
        result = self.finish(process)
        self.assertTrue(result["result"]["successful"])
        self.assertEqual(node.get_parameter("value").value, 2)
        self.assertEqual(validated, [[2]])

    def test_dump_load_retains_uninitialized_and_readonly_records_atomically(self):
        node = self.peers()
        node.declare_parameter("optional", None, aos.ParameterDescriptor(dynamic_typing=True))
        node.declare_parameter("typed_empty", aos.ParameterType.INTEGER)
        node.declare_parameter("locked", 3, aos.ParameterDescriptor(read_only=True))
        node.declare_parameter("movable", 7)
        validations = []
        node.add_on_set_parameters_callback(
            lambda candidate: (validations.append([p.name for p in candidate])
                               or aos.SetParametersResult(True)))
        self.spin(node)
        for kind in ("startup", "snapshot"):
            path = self.directory / f"roundtrip-{kind}.json"
            self.cli("param", "dump", self.request(node, path=str(path), format=kind))
            self.cli("param", "set", self.request(node, parameters={"movable": 8}))
            before = node.get_parameter_snapshot()
            validations.clear()
            result = self.cli("param", "load", self.request(node, path=str(path)))["result"]
            self.assertTrue(result["successful"])
            self.assertEqual(result["version"], before["version"] + 1)
            self.assertEqual(validations, [[
                "locked", "movable", "optional", "typed_empty", "use_sim_time",
            ]])
            self.assertEqual(node.get_parameter("movable").value, 7)
            after = node.get_parameter_snapshot()
            old = {record["name"]: record for record in before["parameters"]}
            current = {record["name"]: record for record in after["parameters"]}
            for name in ("locked", "optional", "typed_empty", "use_sim_time"):
                self.assertEqual(current[name], old[name])
            same = self.cli("param", "load", self.request(node, path=str(path)))["result"]
            self.assertTrue(same["successful"])
            self.assertFalse(same["event_published"])
            self.assertEqual(node.get_parameter_snapshot(), after)

        invalid = self.directory / "readonly-change.json"
        invalid.write_text('{"parameters":{"movable":10,"locked":4}}')
        before = node.get_parameter_snapshot()
        rejected = self.cli("param", "load", self.request(node, path=str(invalid)), exit_code=1)
        self.assertEqual(rejected["code"], "PARAMETER_READ_ONLY")
        self.assertEqual(node.get_parameter_snapshot(), before)
        self.cli("param", "set", self.request(node, parameters={"optional": 9}))
        before = node.get_parameter_snapshot()
        rejected = self.cli("param", "load", self.request(
            node, path=str(self.directory / "roundtrip-snapshot.json")), exit_code=1)
        self.assertEqual(rejected["code"], "PARAMETER_CONFLICT")
        self.assertEqual(node.get_parameter_snapshot(), before)
        self.cli("param", "set", self.request(node, parameters={"optional": None}))
        self.assertFalse(node.has_parameter("optional"), "explicit set null must still delete")

    def test_empty_load_is_a_valid_noop_while_empty_set_is_rejected(self):
        node = self.peers()
        self.spin(node)
        path = self.directory / "empty.json"
        path.write_text('{"parameters":{}}')
        before = node.get_parameter_snapshot()
        loaded = self.cli("param", "load", self.request(node, path=str(path)))["result"]
        self.assertTrue(loaded["successful"])
        self.assertFalse(loaded["event_published"])
        self.assertEqual(node.get_parameter_snapshot(), before)
        rejected = self.cli("param", "set", self.request(node, parameters={}), exit_code=2)
        self.assertEqual(rejected["code"], "INVALID_ARGUMENT")

    def after_listing(self, node, action):
        service = next(entry for entry in node._callbacks if isinstance(entry, _ParameterServices))

        def take():
            request = service._native.recv(0.0)
            kind = request.kind

            def invoke():
                request.execute(node._validate_parameter_candidate)
                if kind == "list_parameters":
                    action()

            return invoke

        service._take_callback = take

    def test_dump_refuses_a_mixed_version_without_writing_or_retrying(self):
        node = self.peers()
        node.declare_parameter("value", 1)
        listings = []

        def change():
            listings.append(True)
            self.assertTrue(node.set_parameters_atomically(
                [aos.Parameter("value", value=2)]).successful)

        self.after_listing(node, change)
        self.spin(node)
        path = self.directory / "dump.json"
        error = self.cli("param", "dump", self.request(node, path=str(path)), exit_code=1)
        self.assertEqual(error["code"], "PARAMETER_CONFLICT")
        self.assertEqual(listings, [True])
        self.assertFalse(path.exists())

    def test_dump_file_error_does_not_claim_a_remote_mutation_failed(self):
        node = self.peers()
        node.declare_parameter("value", 1)
        parent = self.directory / "output"
        parent.mkdir()
        self.after_listing(node, parent.rmdir)
        self.spin(node)
        error = self.cli("param", "dump", self.request(
            node, path=str(parent / "dump.json")), exit_code=1)
        self.assertEqual(error["code"], "FILE_IO_ERROR")
        self.assertEqual(error["phase"], "file_write")
        self.assertEqual(error["remote_execution"], "read-only parameter snapshot received")
        self.assertEqual(node.get_parameter("value").value, 1)

    def test_dump_deleted_after_listing_is_conflict_and_preserves_existing_file(self):
        node = self.peers()
        node.declare_parameter("optional", 1, aos.ParameterDescriptor(dynamic_typing=True))
        listings = []

        def remove():
            listings.append(True)
            result = node.set_parameters_atomically([aos.Parameter("optional", value=None)])
            self.assertTrue(result.successful)

        self.after_listing(node, remove)
        self.spin(node)
        path = self.directory / "existing.json"
        path.write_text("previous complete snapshot\n")
        error = self.cli("param", "dump", self.request(node, path=str(path)), exit_code=1)
        self.assertEqual(error["code"], "PARAMETER_CONFLICT")
        self.assertEqual(listings, [True])
        self.assertFalse(node.has_parameter("optional"))
        self.assertEqual(path.read_text(), "previous complete snapshot\n")
        self.assertEqual(list(self.directory.glob(".aos-*.tmp")), [])

    @unittest.skipUnless(os.name == "posix", "file mode checks use POSIX permissions")
    def test_dump_new_file_is_private_and_replacement_preserves_permissions(self):
        node = self.peers()
        node.declare_parameter("value", 7)
        self.spin(node)
        for mode in (None, 0o600, 0o640):
            path = self.directory / f"mode-{mode}.json"
            if mode is not None:
                path.write_text("old")
                path.chmod(mode)
            result = self.cli("param", "dump", self.request(node, path=str(path)))
            self.assertIsInstance(result["file_write"]["directory_synced"], bool)
            self.assertEqual(path.stat().st_mode & 0o777, mode or 0o600)
            self.assertEqual(json.loads(path.read_text())["parameters"]["value"]["value"], 7)
        self.assertEqual(list(self.directory.glob(".aos-*.tmp")), [])

    def test_lifecycle_queries_id_label_and_parameter_state_contracts(self):
        node = self.peers(aos.LifecycleNode)
        node.declare_parameter("config", 1, aos.ParameterDescriptor(allowed_states=("inactive",)))
        self.spin(node)
        current = self.cli("lifecycle", "get", self.request(node))
        self.assertEqual(current["current_state"]["label"], "unconfigured")
        states = self.cli("lifecycle", "list", self.request(node, view="states"))
        self.assertIn("active", [state["label"] for state in states["available_states"]])
        transitions = self.cli("lifecycle", "list", self.request(node))
        self.assertIn(1, [item["id"] for item in transitions["available_transitions"]])
        graph = self.cli("lifecycle", "list", self.request(node, view="graph"))
        self.assertIn(62, [item["id"] for item in graph["available_transitions"]])
        rejected = self.cli("param", "set", self.request(node, parameters={"config": 2}), exit_code=1)
        self.assertEqual(rejected["code"], "PARAMETER_STATE_NOT_ALLOWED")
        configured = self.cli("lifecycle", "set", self.request(node, transition="configure"))
        self.assertEqual(configured["report"]["final_state"]["label"], "inactive")
        self.cli("param", "set", self.request(node, parameters={"config": 2}))
        activated = self.cli("lifecycle", "set", self.request(node, transition=3))
        self.assertEqual(activated["report"]["final_state"]["label"], "active")
        rejected = self.cli("param", "set", self.request(node, parameters={"config": 3}), exit_code=1)
        self.assertEqual(rejected["code"], "PARAMETER_STATE_NOT_ALLOWED")
        self.cli("lifecycle", "set", self.request(node, transition="deactivate", drain_timeout=0))
        shutdown = self.cli("lifecycle", "set", self.request(node, transition="shutdown"))
        self.assertEqual(shutdown["report"]["final_state"]["label"], "finalized")
        final = self.cli("lifecycle", "get", self.request(node))
        self.assertEqual(final["current_state"]["label"], "finalized")
        self.assertTrue(node.context.ok())

    def test_parameter_timeout_does_not_repeat_or_roll_back_accepted_work(self):
        node = self.peers()
        node.declare_parameter("value", 1)
        entered, release, completed = threading.Event(), threading.Event(), threading.Event()
        calls = []

        def validate(candidate):
            calls.append([p.value for p in candidate])
            entered.set()
            if not release.wait(5):
                return aos.SetParametersResult(False, "test release timed out")
            completed.set()
            return aos.SetParametersResult(True)

        node.add_on_set_parameters_callback(validate)
        self.spin(node)
        self.addCleanup(release.set)
        began = time.monotonic()
        process = self.start("param", "set", self.request(
            node, parameters={"value": 2}, timeout=1))
        self.assertTrue(entered.wait(3))
        error = self.finish(process, 4)
        self.assertEqual(error["code"], "TIMEOUT")
        self.assertLess(time.monotonic() - began, 2)
        self.assertFalse(completed.is_set())
        self.assertEqual(node.get_parameter("value").value, 1)
        release.set()
        self.assertTrue(completed.wait(2))
        deadline = time.monotonic() + 2
        while node.get_parameter("value").value != 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(node.get_parameter("value").value, 2)
        self.assertEqual(calls, [[2]])

    def test_lifecycle_drain_timeout_returns_remaining_work_without_stopping_it(self):
        node = self.peers(aos.LifecycleNode)
        interface = aos.Interface.load(
            Path(__file__).resolve().parents[2] / "interfaces" / "AddTwoInts.json")
        entered, release = threading.Event(), threading.Event()

        def slow(request):
            entered.set()
            if not release.wait(5):
                raise RuntimeError("test release timed out")
            return {"sum": request["a"] + request["b"]}

        node.create_service(interface, "slow", slow)
        self.spin(node)
        self.addCleanup(release.set)
        self.cli("lifecycle", "set", self.request(node, transition="configure"))
        self.cli("lifecycle", "set", self.request(node, transition="activate"))
        caller = aos.Node("caller", self.namespace, self.context)
        self.addCleanup(caller.destroy_node)
        client = caller.create_client(interface, "slow")
        self.assertTrue(client.wait_for_service(2))
        result = client.call_async({"a": 20, "b": 22}, timeout=5)
        self.assertTrue(entered.wait(2))
        error = self.cli("lifecycle", "set", self.request(
            node, transition="deactivate", drain_timeout=0.02), exit_code=1)
        self.assertEqual(error["code"], "LIFECYCLE_DRAIN_TIMEOUT")
        report = error["lifecycle_result"]["report"]
        self.assertTrue(report["drain_timed_out"])
        self.assertEqual(report["final_state"]["label"], "active")
        self.assertEqual([work["kind"] for work in report["remaining"]], ["service"])
        self.assertFalse(result.done())
        release.set()
        self.assertEqual(result.result(2), {"sum": 42})
        completed = self.cli("lifecycle", "set", self.request(
            node, transition="deactivate", drain_timeout=1))
        self.assertEqual(completed["report"]["final_state"]["label"], "inactive")

    def test_sigint_ends_observation_while_transition_and_control_services_continue(self):
        entered, release, completed = threading.Event(), threading.Event(), threading.Event()
        calls = []

        class Controlled(aos.LifecycleNode):
            def on_configure(self, state):
                calls.append(state.label)
                entered.set()
                if not release.wait(5):
                    return aos.TransitionCallbackReturn.FAILURE
                completed.set()
                return aos.TransitionCallbackReturn.SUCCESS

        node = self.peers(Controlled)
        node.declare_parameter("value", 1)
        self.spin(node)
        self.addCleanup(release.set)
        process = self.start("lifecycle", "set", self.request(node, transition="configure", timeout=10))
        self.assertTrue(entered.wait(3))
        current = self.cli("lifecycle", "get", self.request(node))
        self.assertEqual(current["current_state"]["label"], "configuring")
        value = self.cli("param", "get", self.request(node, names=["value"]))
        self.assertEqual(value["parameters"][0]["value"]["value"], 1)
        began = time.monotonic()
        interrupt_process(process, signal.SIGINT)
        error = self.finish(process, 130)
        self.assertEqual(error["code"], "INTERRUPTED")
        self.assertLess(time.monotonic() - began, 1)
        self.assertFalse(completed.is_set())
        self.assertEqual(node.get_current_state().label, "configuring")
        release.set()
        self.assertTrue(completed.wait(2))
        deadline = time.monotonic() + 2
        while node.get_current_state().label == "configuring" and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(node.get_current_state().label, "inactive")
        self.assertEqual(calls, ["unconfigured"])
