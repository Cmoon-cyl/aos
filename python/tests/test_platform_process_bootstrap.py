"""Installed-wheel startup injection through the ordinary SDK constructors.

Run from outside the checkout with Python isolated mode. These tests use a real
Zenoh peer and do not import the repository's Python package or mock a transport.
"""

import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import aos_sdk as aos


LAUNCH_ID = "606c355e-00a9-405b-9f41-c5855426dfe2"


class ProcessBootstrapTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        environment = mock.patch.dict(os.environ)
        environment.start()
        self.addCleanup(environment.stop)
        for key in (
            "AOS_PROCESS_CONFIG", "AOS_DOMAIN_ID", "AOS_ZENOH_CONFIG",
            "AOS_NODE_NAME", "AOS_NAMESPACE", "AOS_USE_SIM_TIME",
        ):
            os.environ.pop(key, None)
        self.peer = self.directory / "peer.json5"
        self.peer.write_text(
            """{
                mode: "peer",
                connect: { endpoints: [] },
                listen: { endpoints: ["tcp/127.0.0.1:0"] },
                scouting: { multicast: { enabled: false }, gossip: { enabled: false } }
            }""",
            encoding="utf-8",
        )
        self.path = self.directory / "bootstrap.json"
        self.document = {
            "schema_version": 1,
            "launch_id": LAUNCH_ID,
            "process_id": "python-worker",
            "generation": 1,
            "context": {"launch": {"zenoh_config": str(self.peer)}},
            "node": {},
        }
        self.write_document()
        os.environ["AOS_PROCESS_CONFIG"] = str(self.path)

    def write_document(self):
        self.path.write_text(json.dumps(self.document), encoding="utf-8")

    def configuration(self, name, value):
        path = self.directory / name
        path.write_text(json.dumps(value), encoding="utf-8")
        return path

    def context(self, **kwargs):
        context = aos.Context(**kwargs)
        self.addCleanup(lambda: context.shutdown() if context.ok() else None)
        return context

    def node(self, name="worker", namespace="/declared", *, managed=False, **kwargs):
        cls = aos.LifecycleNode if managed else aos.Node
        node = cls(name, namespace, **kwargs)
        self.addCleanup(node.destroy_node)
        return node

    def identity(self, generation=1):
        return {
            "launch_id": LAUNCH_ID, "process_id": "python-worker",
            "generation": generation, "pid": os.getpid(),
        }

    def test_ordinary_node_with_implicit_context_consumes_bootstrap(self):
        self.document["node"] = {"launch": {"nodes": {
            "/declared/worker": {"name": "deployed", "namespace": "/runtime"},
        }}}
        self.write_document()
        node = self.node()
        self.assertEqual(node.fully_qualified_name, "/runtime/deployed")
        self.assertEqual(node.get_node_card()["process"], self.identity())
        self.assertEqual(node.context.effective_config["zenoh_config"], {
            "value": str(self.peer), "source": "launch",
        })

    def test_context_snapshot_survives_file_and_locator_changes(self):
        self.document["node"] = {
            "launch": {"nodes": {
                "/declared/worker": {"name": "first", "namespace": "/runtime"},
            }},
            "cli": {"nodes": {
                "/declared/worker": {"parameters": {"rate": 6}},
                "/runtime/first": {"summary": "must not match final identity"},
            }},
        }
        self.write_document()
        first = self.context()
        self.document["generation"] = 2
        self.document["node"]["launch"]["nodes"]["/declared/worker"]["name"] = "second"
        self.write_document()
        second = self.context()
        self.path.write_text("no longer valid JSON", encoding="utf-8")
        os.environ["AOS_PROCESS_CONFIG"] = str(self.directory / "missing.json")

        for managed in (False, True):
            node = self.node(context=first, managed=managed, launch_config={
                "nodes": {"/declared/worker": {"summary": "API summary"}},
            })
            self.assertEqual(node.fully_qualified_name, "/runtime/first")
            self.assertEqual(node.get_node_card()["process"], self.identity())
            self.assertEqual(node.get_node_card()["summary"], "API summary")
            self.assertEqual(node.declare_parameter("rate", 0).value, 6)
        node = self.node(context=second)
        self.assertEqual(node.fully_qualified_name, "/runtime/second")
        self.assertEqual(node.get_node_card()["process"], self.identity(2))
        with self.assertRaises(aos.InvalidArgumentError):
            aos.Context()

    def test_sources_keep_file_order_environment_and_launch_cli_layers(self):
        local_context = self.configuration("local-context.json", {"domain_id": 11})
        launch_context = self.configuration("launch-context.json", {"domain_id": 12})
        local_node = self.configuration("local-node.json", {
            "display_name": "local file", "parameters": {"rate": 2},
        })
        launch_node = self.configuration("launch-node.json", {
            "display_name": "launcher file", "parameters": {"rate": 3},
        })
        self.document["context"].update({
            "config_files": [str(launch_context)], "cli": {"domain_id": 15},
        })
        self.document["context"]["launch"]["domain_id"] = 14
        self.document["node"] = {
            "config_files": [str(launch_node)],
            "launch": {"namespace": "/launch", "parameters": {"rate": 4}},
            "cli": {"name": "cli", "parameters": {"rate": 6}},
        }
        self.write_document()
        os.environ.update({
            "AOS_DOMAIN_ID": "13", "AOS_NODE_NAME": "environment",
            "AOS_NAMESPACE": "/environment", "AOS_USE_SIM_TIME": "true",
            "AOS_NOT_A_SETTING": "ignored",
        })
        context = self.context(domain_id=10, config_files=[local_context])
        node = self.node(
            context=context, summary="program summary", config_files=[local_node],
            parameter_overrides={"rate": 1},
        )
        self.assertEqual(context.effective_config["domain_id"], {
            "value": 15, "source": "cli",
        })
        expected = {
            "tags": ([], "program"),
            "summary": ("program summary", "program"),
            "display_name": ("launcher file", f"file:{launch_node}"),
            "use_sim_time": (True, "environment"),
            "namespace": ("/launch", "launch"),
            "name": ("cli", "cli"),
        }
        for field, (value, source) in expected.items():
            self.assertEqual(node.effective_config[field], {
                "value": value, "source": source,
            })
        self.assertFalse(node.has_parameter("rate"))
        self.assertEqual(node.declare_parameter("rate", 0).value, 6)
        records = {record["name"]: record for record in node.get_parameter_snapshot()["parameters"]}
        self.assertEqual(records["rate"]["source"], "cli")

    def test_same_source_conflicts_fail_and_disjoint_fields_compose(self):
        self.document["context"]["launch"]["domain_id"] = 31
        self.document["node"] = {"launch": {
            "nodes": {"/declared/worker": {"summary": "same", "parameters": {"rate": 6}}},
        }}
        self.write_document()
        with self.assertRaisesRegex(aos.InvalidArgumentError, "overlap"):
            aos.Context(launch_config={"domain_id": 31})
        context = self.context()
        for launch in (
            {"nodes": {"/declared/worker": {"summary": "same"}}},
            {"nodes": {"/declared/worker": {"parameters": {"rate": 6}}}},
        ):
            with self.subTest(launch=launch):
                with self.assertRaisesRegex(aos.InvalidArgumentError, "overlap"):
                    aos.Node("worker", "/declared", context=context, launch_config=launch)
        node = self.node(context=context, launch_config={
            "nodes": {"/declared/worker": {"display_name": "API display"}},
        })
        self.assertEqual(node.get_node_card()["display_name"], "API display")
        self.assertEqual(node.get_node_card()["summary"], "same")

    def test_startup_overrides_preserve_parameter_type_range_and_lifecycle_policy(self):
        self.document["node"] = {"cli": {"parameters": {
            "rate": 6, "wrong_type": "text", "outside_range": 11, "validator": 7,
        }}}
        self.write_document()
        context = self.context()
        node = self.node(context=context, managed=True)
        descriptor = aos.ParameterDescriptor(
            description="Only adjust while inactive",
            integer_range=aos.IntegerRange(0, 10, 2),
            allowed_states=("inactive",),
        )
        self.assertEqual(node.get_current_state().label, "unconfigured")
        self.assertFalse(node.has_parameter("rate"))
        self.assertEqual(node.declare_parameter("rate", 0, descriptor).value, 6)
        self.assertEqual(node.describe_parameter("rate").allowed_states, ("inactive",))
        self.assertEqual(
            node.set_parameters_atomically([aos.Parameter("rate", value=8)]).code,
            "state_not_allowed",
        )
        for name, expected in (("wrong_type", "wrong_type"), ("outside_range", "out_of_range")):
            with self.subTest(name=name), self.assertRaises(aos.ParameterError) as rejected:
                node.declare_parameter(name, 0, aos.ParameterDescriptor(
                    integer_range=aos.IntegerRange(0, 10),
                ))
            self.assertEqual(rejected.exception.code, expected)
            self.assertFalse(node.has_parameter(name))
        calls = []

        def validate(candidate):
            calls.append([(parameter.name, parameter.value) for parameter in candidate])
            return aos.SetParametersResult(False, "rejected startup override")

        node.add_on_set_parameters_callback(validate)
        with self.assertRaises(aos.ParameterError) as rejected:
            node.declare_parameter("validator", 0)
        self.assertEqual(rejected.exception.code, "validation")
        self.assertEqual(calls, [[("validator", 7)]])
        self.assertFalse(node.has_parameter("validator"))

    def test_invalid_bootstrap_fails_before_zenoh_config_is_opened(self):
        cases = [
            '{"schema_version":1,"schema_version":1}',
            json.dumps({**self.document, "unknown": True}),
            json.dumps({**self.document, "generation": 0}),
            json.dumps({**self.document, "context": {"launch": {
                "zenoh_config": "relative.json5",
            }}}),
            json.dumps({**self.document, "node": {"launch": {
                "nodes": {"unmatched": {"unknown": True}},
            }}}),
        ]
        for text in cases:
            with self.subTest(text=text):
                self.path.write_text(text, encoding="utf-8")
                with self.assertRaises(aos.InvalidArgumentError) as rejected:
                    aos.Context(config=self.directory / "missing-session.json5")
                self.assertNotIn("missing-session.json5", str(rejected.exception))

    def test_no_bootstrap_keeps_optional_metadata_absent(self):
        os.environ.pop("AOS_PROCESS_CONFIG")
        context = self.context(config=self.peer)
        node = self.node(context=context)
        self.assertNotIn("process", node.get_node_card())
        self.assertEqual(node.effective_config["use_sim_time"], {
            "value": False, "source": "default",
        })


if __name__ == "__main__":
    unittest.main()
