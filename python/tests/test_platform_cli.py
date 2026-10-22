"""CLI configuration and QoS acceptance against installed-wheel SDK peers."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import time
import unittest
from unittest import mock
import uuid

import aos_sdk as aos
from test_stage1_cli import RouterProcess

ROOT = Path(__file__).resolve().parents[2]


class PlatformCliTest(unittest.TestCase):
    def setUp(self):
        self.cli_path = os.environ["AOS_CLI"]
        self.assertTrue(Path(self.cli_path).is_file())
        temporary = tempfile.TemporaryDirectory(prefix="aos-platform-cli-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.environment = dict(os.environ)
        for key in ("AOS_NODE_NAME", "AOS_NAMESPACE", "AOS_USE_SIM_TIME",
                    "AOS_ZENOH_CONFIG", "AOS_DOMAIN_ID", "AOS_INTERFACE_PATH"):
            self.environment.pop(key, None)

    def cli(self, group, verb, request=None, *, exit_code=0, environment=None, raw=None):
        process = subprocess.run(
            [self.cli_path, group, verb, "--request", "-"],
            input=raw if raw is not None else json.dumps(request or {}),
            capture_output=True, text=True, timeout=15,
            env=self.environment | (environment or {}),
        )
        self.assertEqual(process.returncode, exit_code, process.stdout + process.stderr)
        messages = [json.loads(line) for line in process.stdout.splitlines()]
        self.assertTrue(messages, process.stderr)
        if (group == "topic" and verb in ("pub", "echo")) or (group, verb) == ("action", "send_goal"):
            return messages
        self.assertEqual(len(messages), 1, messages)
        self.assertEqual(messages[0]["ok"], exit_code == 0)
        return messages[0]["data" if exit_code == 0 else "error"]

    def config(self, name, value):
        path = self.directory / name
        path.write_text(json.dumps(value), encoding="utf-8")
        return str(path)

    def peers(self):
        router = RouterProcess()
        self.addCleanup(router.close)
        patch = mock.patch.dict(os.environ, self.environment, clear=True)
        patch.start()
        self.addCleanup(patch.stop)
        context = aos.Context(config=router.config, domain_id=91)
        self.addCleanup(context.shutdown)
        namespace = "/platform_cli_" + uuid.uuid4().hex
        node = aos.Node("provider", namespace, context)
        self.addCleanup(node.destroy_node)
        self.environment.update(AOS_ZENOH_CONFIG=str(router.config), AOS_DOMAIN_ID="91")
        return router, context, node, namespace

    def spin(self, context, *nodes):
        executor = aos.MultiThreadedExecutor(4, context=context)
        for node in nodes:
            executor.add_node(node)
        spinner = threading.Thread(target=executor.spin, name="platform-cli-executor")
        spinner.start()

        def close():
            self.assertTrue(executor.shutdown(timeout_sec=5))
            spinner.join(5)
            self.assertFalse(spinner.is_alive())

        self.addCleanup(close)

    def test_offline_precedence_and_kind_scoped_one_pass_remap(self):
        first = self.config("first.json", {"name": "file_first", "summary": "retained"})
        second = self.config("second.json", {"name": "file_second"})
        request = {
            "declared_name": "worker", "declared_namespace": "/original",
            "endpoint_name": "~/input", "kind": "service",
            "node_config": {
                "config_files": [first, second],
                "launch": {"name": "launched", "namespace": "/launch"},
                "cli": {"name": "deployed", "namespace": "/final", "remaps": [
                    {"node": "/original/worker", "kind": "service",
                     "from": "~/input", "to": "output"},
                    {"kind": "service", "from": "output", "to": "never"},
                    {"kind": "topic", "from": "~/input", "to": "topic_output"},
                ]},
            },
            "context_config": {"launch": {"domain_id": 10}, "cli": {"domain_id": 11}},
        }
        environment = {"AOS_NODE_NAME": "environment", "AOS_DOMAIN_ID": "9",
                       "AOS_ZENOH_CONFIG": str(self.directory / "does-not-exist.json")}
        result = self.cli("config", "resolve", request, environment=environment)
        self.assertFalse(result["network_opened"])
        self.assertEqual(result["node"]["name"], {"value": "deployed", "source": "cli"})
        self.assertEqual(result["node"]["summary"]["value"], "retained")
        self.assertEqual(result["context"]["domain_id"], {"value": 11, "source": "cli"})
        self.assertEqual(result["name_resolution"]["original_name"], "~/input")
        self.assertEqual(result["name_resolution"]["resolved_name"], "/final/output")
        self.assertTrue(result["name_resolution"]["remap_source"].startswith("cli"))
        request["kind"] = "topic"
        self.assertEqual(self.cli("config", "resolve", request, environment=environment)
                         ["name_resolution"]["resolved_name"], "/final/topic_output")
        del request["node_config"]["cli"]
        self.assertEqual(self.cli("config", "resolve", request, environment=environment)
                         ["node"]["name"]["source"], "launch")
        del request["node_config"]["launch"]
        self.assertEqual(self.cli("config", "resolve", request, environment=environment)
                         ["node"]["name"]["source"], "environment")
        environment.pop("AOS_NODE_NAME")
        self.assertEqual(self.cli("config", "resolve", request, environment=environment)
                         ["node"]["name"], {"value": "file_second", "source": f"file:{second}"})

    def test_offline_empty_node_environment_preserves_file_values(self):
        config = self.config("empty_env.json", {
            "name": "file_worker", "namespace": "/file_ns", "use_sim_time": True,
        })
        result = self.cli("config", "resolve", {
            "declared_name": "worker", "declared_namespace": "/program",
            "node_config": {"config_files": [config]},
        }, environment={
            "AOS_NODE_NAME": "", "AOS_NAMESPACE": "", "AOS_USE_SIM_TIME": "",
            "AOS_ZENOH_CONFIG": str(self.directory / "does-not-exist.json"),
        })
        self.assertFalse(result["network_opened"])
        for key, value in (
            ("name", "file_worker"), ("namespace", "/file_ns"), ("use_sim_time", True),
        ):
            self.assertEqual(result["node"][key], {
                "value": value, "source": f"file:{config}",
            })

    def test_offline_shared_nested_selectors_have_loader_independent_validity(self):
        config = self.config("shared.json", {
            "nodes": {"other": {"remaps": [
                {"node": "/ns/other", "from": "in", "to": "out"},
            ]}},
        })
        for name, namespace, expected in (
            ("worker", "/a", "/a/in"), ("other", "/ns", "/ns/out"), ("other", "/x", "/x/in"),
        ):
            result = self.cli("config", "resolve", {
                "declared_name": name, "declared_namespace": namespace,
                "node_config": {"config_files": [config]},
                "endpoint_name": "in", "kind": "topic",
            })
            self.assertFalse(result["network_opened"])
            self.assertEqual(result["name_resolution"]["resolved_name"], expected)
        self.config("shared.json", {
            "nodes": {"other": {"remaps": [
                {"node": "/ns/different", "from": "in", "to": "out"},
            ]}},
        })
        for name, namespace in (("worker", "/a"), ("other", "/ns"), ("other", "/x")):
            error = self.cli("config", "resolve", {
                "declared_name": name, "declared_namespace": namespace,
                "node_config": {"config_files": [config]},
            }, exit_code=2)
            self.assertEqual(error["code"], "INVALID_ARGUMENT")
            self.assertIn("selector conflicts", error["message"])

    def test_invalid_contracts_fail_before_opening_network(self):
        bad_network = {"AOS_ZENOH_CONFIG": str(self.directory / "missing.json")}
        cases = [
            ("service", "call", {"service_name": "/add", "service_type": "x/srv/Y",
                                 "values": {}, "qos": {"reliability": "best_effort"}}),
            ("topic", "echo", {"topic_name": "/topic", "qos": {"deadline": 1}}),
            ("action", "send_goal", {"action_name": "/work", "action_type": "x/action/Y",
                                     "goal": {}, "qos": {"status": {"depth": 0}}}),
            ("config", "resolve", {"node_config": {"cli": {"domain_id": 1}}}),
        ]
        for group, verb, request in cases:
            with self.subTest(group=group, request=request):
                response = self.cli(group, verb, request, exit_code=2, environment=bad_network)
                error = response[-1]["data"] if isinstance(response, list) else response
                self.assertEqual(error["code"], "INVALID_ARGUMENT")
                self.assertNotIn("session configuration:", error["message"])
        duplicate = self.cli("config", "resolve", raw='{"declared_name":"a","declared_name":"b"}',
                             exit_code=2, environment=bad_network)
        self.assertIn("duplicate", duplicate["message"].lower())

    def test_service_remap_and_context_override_reach_actual_provider_once(self):
        router, context, node, namespace = self.peers()
        interface = aos.Interface.load(ROOT / "interfaces/AddTwoInts.json")
        calls = []

        def add(request):
            calls.append(request)
            return {"sum": request["a"] + request["b"]}

        node.create_service(interface, "add", add, qos_profile=aos.QoSProfile(depth=4))
        self.spin(context, node)
        request = {
            "service_name": "alias", "service_type": interface.type_name,
            "values": {"a": 20, "b": 22}, "timeout": 3, "qos": {"depth": 3},
            "node_config": {"cli": {"namespace": namespace, "remaps": [
                {"kind": "service", "from": "alias", "to": "add"},
                {"kind": "service", "from": "add", "to": "wrong_if_remapped_twice"},
            ]}},
            "context_config": {"cli": {"domain_id": 91, "zenoh_config": str(router.config)}},
        }
        result = self.cli("service", "call", request, environment={
            "AOS_DOMAIN_ID": "92", "AOS_ZENOH_CONFIG": str(self.directory / "wrong.json"),
        })
        self.assertEqual(result["response"], {"sum": 42})
        self.assertEqual(result["name_resolution"]["resolved_name"], namespace + "/add")
        self.assertEqual(result["qos"]["depth"], 3)
        self.assertEqual(calls, [{"a": 20, "b": 22}])
        info = self.cli("service", "info", {"service_name": namespace + "/add"})
        self.assertEqual(info["qos_endpoints"][0]["qos"]["depth"], 4)

    def test_cli_identity_cannot_shadow_provider_and_diagnostics_ignore_node_environment(self):
        _, context, node, namespace = self.peers()
        message = aos.Interface.load(ROOT / "interfaces/String.json")
        service = aos.Interface.load(ROOT / "interfaces/AddTwoInts.json")
        publisher = node.create_publisher(message, "retained", aos.QoSProfile(preset="action_status"))
        publisher.publish({"data": "identity probe"})
        node.create_service(service, "add", lambda value: {"sum": value["a"] + value["b"]})
        self.spin(context, node)
        environment = self.environment | {
            "AOS_NODE_NAME": "provider", "AOS_NAMESPACE": namespace,
            "AOS_USE_SIM_TIME": "invalid-but-irrelevant-to-online-cli",
        }
        request = {
            "topic_name": "retained", "follow": True, "qos": "action_status", "timeout": 3,
            "declared_name": "observer",
            "node_config": {"cli": {"nodes": {"observer": {"name": "provider"}}}},
        }
        observer = subprocess.Popen(
            [self.cli_path, "topic", "echo", "--request", "-"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, env=environment,
        )
        observer.stdin.write(json.dumps(request))
        observer.stdin.close()
        observer.stdin = None

        def close():
            if observer.poll() is None:
                observer.terminate()
            observer.communicate(timeout=5)

        self.addCleanup(close)
        deadline = time.monotonic() + 5
        while True:
            graph = context.graph(timeout=0.1)
            subscribers = [
                item for item in graph
                if item["kind"] == "subscription"
                and item["endpoint"]["name"] == namespace + "/retained"
            ]
            if subscribers:
                break
            self.assertIsNone(observer.poll(), "observer exited before creating its subscription")
            self.assertLess(time.monotonic(), deadline, "CLI subscription did not appear")
        self.assertTrue(all(item["node_name"].startswith("aos_cli_") for item in subscribers))
        self.assertTrue(all(item["namespace"] == "/" for item in subscribers))
        provider_nodes = [item for item in graph if item["kind"] == "node"
                          and item["node_name"] == "provider" and item["namespace"] == namespace]
        self.assertEqual(len(provider_nodes), 1)
        info = self.cli("node", "info", {"node_name": namespace + "/provider"},
                        environment=environment)
        self.assertIn(namespace + "/add", [item["name"] for item in info["endpoints"]])
        # A live observer must not answer the provider's type introspection query.
        reply = self.cli("service", "call", {
            "service_name": "add", "service_type": service.type_name,
            "values": {"a": 20, "b": 22}, "timeout": 3,
        }, environment=environment)
        self.assertEqual(reply["response"], {"sum": 42})
        invalid_environment = {
            "AOS_NODE_NAME": "bad-name", "AOS_NAMESPACE": "/bad-name",
            "AOS_USE_SIM_TIME": "not-a-boolean",
        }
        topics = self.cli("topic", "list", environment=invalid_environment)
        self.assertIn(namespace + "/retained", [item["name"] for item in topics["topics"]])
        diagnostics = self.cli("doctor", "report", environment=invalid_environment)
        self.assertGreater(diagnostics["graph_entities"], 0)
        self.assertTrue(diagnostics["router_ids"])

    def test_offline_any_resolution_separates_kind_specific_rules(self):
        result = self.cli("config", "resolve", {
            "endpoint_name": "/input",
            "node_config": {"cli": {"remaps": [
                {"kind": "topic", "from": "/input", "to": "/topic"},
                {"kind": "service", "from": "/input", "to": "/service"},
            ]}},
        })
        self.assertEqual(result["name_resolution"]["resolved_name"], "/input")
        self.assertIn("generic", result["name_resolution"]["scope"])
        self.assertEqual(result["name_resolution_by_kind"]["topic"]["resolved_name"], "/topic")
        self.assertEqual(result["name_resolution_by_kind"]["service"]["resolved_name"], "/service")
        self.assertEqual(result["name_resolution_by_kind"]["action"]["resolved_name"], "/input")

    def test_late_echo_uses_transient_profile_and_info_reports_incompatibility(self):
        _, _, node, namespace = self.peers()
        interface = aos.Interface.load(ROOT / "interfaces/String.json")
        publisher = node.create_publisher(interface, "retained", aos.QoSProfile(
            depth=2, durability=aos.DurabilityPolicy.TRANSIENT_LOCAL,
        ))
        publisher.publish({"data": "sent before CLI process exists"})
        events = self.cli("topic", "echo", {
            "topic_name": namespace + "/retained", "qos": "action_status", "once": True, "timeout": 3,
        })
        self.assertEqual(events[0]["event"], "message")
        self.assertEqual(events[0]["data"]["message"], {"data": "sent before CLI process exists"})
        self.assertEqual(events[0]["data"]["qos"]["durability"], "transient_local")
        node.create_publisher(interface, "mismatch", aos.QoSProfile(preset="sensor_data"))
        node.create_subscription(interface, "mismatch", qos_profile=aos.QoSProfile(preset="action_status"))
        info = self.cli("topic", "info", {"topic_name": namespace + "/mismatch"})
        self.assertEqual(len(info["qos_compatibility"]), 1)
        self.assertEqual(info["qos_compatibility"][0]["diagnosis"]["compatibility"], "incompatible")
        self.assertEqual(info["qos_compatibility"][0]["diagnosis"]["enforcement"], "diagnostic_only")

    def test_action_whole_name_remap_and_five_channel_qos(self):
        _, context, node, namespace = self.peers()
        interface = aos.Interface.load(ROOT / "interfaces/action/Fibonacci.json")

        def execute(goal):
            goal.succeed()
            return {"sequence": [0, 1, 1]}

        aos.ActionServer(node, interface, "compute", execute,
                         goal_service_qos_profile=aos.QoSProfile(depth=2),
                         result_service_qos_profile=aos.QoSProfile(depth=3),
                         cancel_service_qos_profile=aos.QoSProfile(depth=4),
                         feedback_qos_profile=aos.QoSProfile(depth=5),
                         status_qos_profile=aos.QoSProfile(preset="action_status", depth=6))
        self.spin(context, node)
        qos = {"goal_service": 7, "result_service": 8, "cancel_service": 9,
               "feedback": {"depth": 10}, "status": {"depth": 11}}
        events = self.cli("action", "send_goal", {
            "action_name": "alias", "action_type": interface.type_name,
            "goal": {"order": 3}, "timeout": 3, "qos": qos,
            "node_config": {"cli": {"namespace": namespace, "remaps": [
                {"kind": "action", "from": "alias", "to": "compute"},
                {"kind": "action", "from": "compute", "to": "wrong_if_remapped_twice"},
            ]}},
        })
        accepted = next(event["data"] for event in events if event["event"] == "goal_response")
        self.assertTrue(accepted["accepted"])
        self.assertEqual(accepted["name_resolution"]["resolved_name"], namespace + "/compute")
        self.assertEqual([accepted["qos"][key]["depth"] for key in qos], [7, 8, 9, 10, 11])
        result = next(event["data"] for event in events if event["event"] == "result")
        self.assertEqual(result["status"], int(aos.GoalStatus.SUCCEEDED))
        self.assertEqual(result["result"], {"sequence": [0, 1, 1]})
        info = self.cli("action", "info", {"action_name": namespace + "/compute"})
        profiles = {entry["endpoint"]["name"].rsplit("/", 1)[-1]: entry["qos"]
                    for entry in info["qos_endpoints"]}
        self.assertEqual({key: value["depth"] for key, value in profiles.items()},
                         {"send_goal": 2, "get_result": 3, "cancel_goal": 4, "feedback": 5, "status": 6})
        self.assertEqual(profiles["status"]["durability"], "transient_local")


if __name__ == "__main__":
    unittest.main()
