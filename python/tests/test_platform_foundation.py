"""Installed-wheel platform checks with an isolated, real Zenoh peer.

Run with the wheel installed in the selected interpreter; no source-path import
or mock transport is used. TCP listeners bind only to an ephemeral loopback port.
"""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

import aos_sdk as aos


ROOT = Path(__file__).resolve().parents[2]


class PlatformTestCase(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        peer = self.directory / "peer.json5"
        peer.write_text(
            """{
              mode: "peer",
              connect: { endpoints: [] },
              listen: { endpoints: ["tcp/127.0.0.1:0"] },
              scouting: { multicast: { enabled: false }, gossip: { enabled: false } },
            }""",
            encoding="utf-8",
        )
        environment = mock.patch.dict(os.environ)
        environment.start()
        self.addCleanup(environment.stop)
        for key in ("AOS_NODE_NAME", "AOS_NAMESPACE", "AOS_USE_SIM_TIME", "AOS_DOMAIN_ID"):
            os.environ.pop(key, None)
        os.environ["AOS_ZENOH_CONFIG"] = str(peer)
        self.context = aos.Context(config=peer, domain_id=73)

        def close_context():
            try:
                self.context.shutdown()
            except aos.InvalidStateError:
                pass

        self.addCleanup(close_context)

    def node(self, *args, **kwargs):
        node = aos.Node(*args, context=self.context, **kwargs)

        def close():
            try:
                node.destroy_node()
            except aos.InvalidStateError:
                pass

        self.addCleanup(close)
        return node

    def config(self, name, value):
        path = self.directory / name
        path.write_text(json.dumps(value), encoding="utf-8")
        return path

    def spin(self, *nodes):
        executor = aos.MultiThreadedExecutor(4, context=self.context)
        for node in nodes:
            executor.add_node(node)
        spinner = threading.Thread(target=executor.spin, name="platform-test-spin")
        spinner.start()

        def stop():
            executor.shutdown(timeout_sec=5)
            spinner.join(5)
            self.assertFalse(spinner.is_alive())

        self.addCleanup(stop)
        return executor

    def wait_for(self, predicate, timeout=3):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(0.005)
        self.fail("condition did not become true before the deadline")


class ConfigurationTest(PlatformTestCase):
    def test_nested_absolute_remap_keeps_scope_with_same_short_name_nodes(self):
        config = self.config("shared-selector.json", {"nodes": {"other": {"remaps": [
            {"node": "/ns/other", "from": "in", "to": "out"},
        ]}}})
        intended = self.node("other", "/ns", config_files=[config])
        sibling = self.node("other", "/x", config_files=[config])
        self.assertEqual(intended.resolve_name("in", kind="topic"), "/ns/out")
        self.assertEqual(sibling.resolve_name("in", kind="topic"), "/x/in")
        interface = aos.Interface.load(ROOT / "interfaces/String.json")
        intended.create_publisher(interface, "in")
        sibling.create_publisher(interface, "in")
        def discovered():
            endpoints = [entity["endpoint"]["name"] for entity in self.context.graph(timeout=0.1)
                         if entity["kind"] == "publisher"]
            return endpoints if len(endpoints) >= 2 else None

        endpoints = self.wait_for(discovered)
        self.assertIn("/ns/out", endpoints)
        self.assertIn("/x/in", endpoints)
        self.assertNotIn("/x/out", endpoints)

    def test_empty_node_environment_values_preserve_declared_and_file_values(self):
        config = self.config("empty_env.json", {
            "name": "file_worker", "namespace": "/file_ns", "use_sim_time": True,
        })
        with mock.patch.dict(os.environ, {
            "AOS_NODE_NAME": "", "AOS_NAMESPACE": "", "AOS_USE_SIM_TIME": "",
        }):
            plain = self.node("worker", "/program")
            configured = self.node("worker", "/program", config_files=[config])
        self.assertEqual((plain.name, plain.namespace), ("worker", "/program"))
        self.assertEqual(plain.effective_config["use_sim_time"], {
            "value": False, "source": "default",
        })
        self.assertEqual((configured.name, configured.namespace), ("file_worker", "/file_ns"))
        for key, value in (
            ("name", "file_worker"), ("namespace", "/file_ns"), ("use_sim_time", True),
        ):
            self.assertEqual(configured.effective_config[key], {
                "value": value, "source": f"file:{config}",
            })

    def test_context_config_sources_resolve_before_opening_session(self):
        first = self.config("context_first.json", {"domain_id": 11})
        second = self.config("context_second.json", {"domain_id": 12})
        with aos.Context(domain_id=10, config_files=[first, second]) as context:
            self.assertEqual(context.effective_config["domain_id"], {
                "value": 12, "source": f"file:{second}",
            })
        with mock.patch.dict(os.environ, {"AOS_DOMAIN_ID": "13"}):
            with aos.Context(domain_id=10, config_files=[first, second]) as context:
                self.assertEqual(context.effective_config["domain_id"], {
                    "value": 13, "source": "environment",
                })
                self.assertEqual(context.effective_config["zenoh_config"], {
                    "value": os.environ["AOS_ZENOH_CONFIG"], "source": "environment",
                })
            with aos.Context(
                domain_id=10, config_files=[first, second],
                launch_config={"domain_id": 14}, cli_overrides={"domain_id": 15},
            ) as context:
                self.assertEqual(context.effective_config["domain_id"], {
                    "value": 15, "source": "cli",
                })
                self.assertTrue(context.ok())
        with mock.patch.dict(os.environ, {"AOS_DOMAIN_ID": "invalid after Context open"}):
            node = self.node("shared_context")
            self.assertTrue(node.context.ok())
            self.assertEqual(node.context.effective_config["domain_id"], {
                "value": 73, "source": "program",
            })

    def test_context_rejects_unsupported_fields_and_bool_domain(self):
        for value in (True, False, -1, 2**32, 1.5, "3"):
            with self.subTest(value=value), self.assertRaises(aos.InvalidArgumentError):
                aos.Context(domain_id=value)
        with self.assertRaises(aos.InvalidArgumentError):
            aos.Context(launch_config={"namespace": "/not_a_context_setting"})
        duplicate = self.directory / "context_duplicate.json"
        duplicate.write_text('{"domain_id":1,"domain_id":2}', encoding="utf-8")
        with self.assertRaises(aos.InvalidArgumentError):
            aos.Context(config_files=[duplicate])

    def test_installed_example_uses_configuration_without_source_imports(self):
        config = self.config("deployment.json", {
            "name": "worker_east", "namespace": "/east",
            "remaps": [
                {"kind": "service", "from": "~/add", "to": "add"},
                {"kind": "service", "from": "add", "to": "must_not_chain"},
            ],
        })
        child = subprocess.run(
            [sys.executable, "-I", str(ROOT / "examples/platform_foundation.py"),
             "--node-config", str(config)],
            cwd=self.directory, capture_output=True, text=True, timeout=15,
        )
        self.assertEqual(child.returncode, 0, child.stderr)
        output = json.loads(child.stdout)
        self.assertEqual(output["node"], "/east/worker_east")
        self.assertEqual(output["service"]["resolved_name"], "/east/add")
        self.assertEqual(output["result"], {"sum": 42})
        self.assertEqual(output["clock"]["type"], "ros")
        self.assertGreaterEqual(
            output["timer"]["actual_call_time"], output["timer"]["expected_call_time"],
        )

    def test_configuration_layers_keep_values_and_sources(self):
        first = self.config("first.json", {
            "name": "file_first", "namespace": "/first",
            "summary": "from first file", "tags": ["first", "retained"],
        })
        second = self.config("second.json", {
            "name": "file_second", "tags": ["second"],
        })
        plain = self.node("worker", "/program")
        self.assertEqual(plain.effective_config["use_sim_time"], {
            "value": False, "source": "default",
        })
        self.assertEqual(plain.effective_config["namespace"], {
            "value": "/program", "source": "program",
        })
        file_node = self.node("worker", "/program", config_files=[first, second])
        self.assertEqual(file_node.name, "file_second")
        self.assertEqual(file_node.namespace, "/first")
        self.assertEqual(file_node.effective_config["name"], {
            "value": "file_second", "source": f"file:{second}",
        })
        self.assertEqual(file_node.get_node_card()["tags"], ["second"])

        with mock.patch.dict(os.environ, {
            "AOS_NODE_NAME": "environment", "AOS_NAMESPACE": "/environment",
            "AOS_USE_SIM_TIME": "true", "AOS_UNRECOGNIZED_SETTING": "ignored",
        }):
            env_node = self.node("worker", config_files=[first, second])
            launch_node = self.node(
                "worker", config_files=[first, second],
                launch_config={"name": "launch", "namespace": "/launch"},
            )
            cli_node = self.node(
                "worker", config_files=[first, second],
                launch_config={"name": "launch", "namespace": "/launch"},
                cli_overrides={"name": "cli", "namespace": "/cli"},
            )
        self.assertEqual(env_node.effective_config["name"], {
            "value": "environment", "source": "environment",
        })
        self.assertEqual(launch_node.effective_config["name"], {
            "value": "launch", "source": "launch",
        })
        self.assertEqual(cli_node.effective_config["name"], {
            "value": "cli", "source": "cli",
        })
        self.assertEqual(cli_node.effective_config["use_sim_time"], {
            "value": True, "source": "environment",
        })
        self.assertEqual(cli_node.get_node_card()["summary"], "from first file")
        self.assertEqual(cli_node.resolve_name("~/status"), "/cli/cli/status")
        self.assertEqual(cli_node.effective_config["declared_identity"], {
            "name": "worker", "namespace": "/",
        })

    def test_declared_identity_selects_scoped_rules_after_rename(self):
        node = self.node(
            "worker", "/original",
            remaps=[{"from": "~/input", "to": "~/program"}],
            launch_config={
                "name": "renamed", "namespace": "/deployment",
                "nodes": {
                    "worker": {"summary": "declared name selected"},
                    "renamed": {"summary": "must not select final name"},
                },
                "remaps": [
                    {"from": "~/input", "to": "~/generic"},
                    {"node": "/original/worker", "from": "~/input", "to": "~/selected"},
                    {"from": "~/selected", "to": "~/never"},
                    {"node": "other", "from": "~/input", "to": "~/other"},
                ],
            },
        )
        self.assertEqual(node.get_node_card()["summary"], "declared name selected")
        details = node.resolve_name_details("~/input", kind="topic")
        self.assertEqual(details, {
            "original_name": "~/input",
            "expanded_name": "/deployment/renamed/input",
            "resolved_name": "/deployment/renamed/selected",
            "kind": "topic",
            "remap_source": "launch",
        })
        self.assertEqual(node.resolve_name("~/selected"), "/deployment/renamed/never")

    def test_kind_scopes_and_final_identity_reach_real_entities(self):
        node = self.node(
            "worker", "/original",
            cli_overrides={
                "name": "deployed", "namespace": "/runtime",
                "remaps": [
                    {"kind": "topic", "from": "input", "to": "topic"},
                    {"kind": "service", "from": "input", "to": "service"},
                ],
            },
        )
        self.assertEqual(node.resolve_name("input", kind="topic"), "/runtime/topic")
        self.assertEqual(node.resolve_name("input", kind="service"), "/runtime/service")
        self.assertEqual(node.resolve_name("input", kind="action"), "/runtime/input")
        self.assertEqual(node.resolve_name("{ns}/{node}/state"), "/runtime/deployed/state")
        self.assertEqual(node.resolve_name("/absolute"), "/absolute")
        message = aos.Interface.load(ROOT / "interfaces/String.json")
        service = aos.Interface.load(ROOT / "interfaces/AddTwoInts.json")
        node.create_publisher(message, "input")
        node.create_service(service, "input", lambda req: {"sum": req["a"] + req["b"]})
        names = {entry["name"] for entry in node.get_node_card()["endpoints"]}
        self.assertIn("/runtime/topic", names)
        self.assertIn("/runtime/service", names)
        graph = self.context.graph(timeout=0.1)
        own = [entry for entry in graph if entry["node_name"] == "deployed"]
        self.assertTrue(own)
        self.assertTrue(all(entry["namespace"] == "/runtime" for entry in own))
        endpoints = {entry["endpoint"]["name"] for entry in own if entry["endpoint"]}
        self.assertIn("/runtime/topic", endpoints)
        self.assertIn("/runtime/service", endpoints)
        self.assertNotIn("/original/input", endpoints)

    def test_two_deployments_reuse_the_same_service_code(self):
        interface = aos.Interface.load(ROOT / "interfaces/AddTwoInts.json")
        caller = self.node("caller")
        providers = []
        for suffix, offset in (("east", 100), ("west", 200)):
            node = self.node("worker", launch_config={
                "name": f"worker_{suffix}", "namespace": f"/{suffix}",
                "remaps": [{"kind": "service", "from": "~/add", "to": "add"}],
            })
            node.create_service(
                interface, "~/add",
                lambda req, offset=offset: {"sum": req["a"] + req["b"] + offset},
            )
            providers.append(node)
        self.spin(caller, *providers)
        for suffix, total in (("east", 103), ("west", 203)):
            client = caller.create_client(interface, f"/{suffix}/add")
            self.assertTrue(client.wait_for_service(timeout_sec=2))
            self.assertEqual(client.call_async({"a": 1, "b": 2}).result(3), {"sum": total})

    def test_invalid_configuration_is_rejected_before_graph_registration(self):
        duplicate = self.directory / "duplicate.json"
        duplicate.write_text('{"name":"one","name":"two"}', encoding="utf-8")
        duplicate_nested = self.directory / "duplicate_nested.json"
        duplicate_nested.write_text(
            '{"nodes":{"worker":{"summary":"one","summary":"two"}}}', encoding="utf-8",
        )
        cases = [
            {"config_files": [duplicate]},
            {"config_files": [duplicate_nested]},
            {"launch_config": {"unknown": 1}},
            {"launch_config": {"use_sim_time": 1}},
            {"launch_config": {"name": None}},
            {"launch_config": {"nodes": {"other": {"unknown": 1}}}},
            {"remaps": [{"from": "*/input", "to": "output"}]},
            {"remaps": [{"from": "input", "to": "output", "unexpected": 1}]},
            {"remaps": [{"kind": "invalid", "from": "input", "to": "output"}]},
            {"remaps": [
                {"from": "input", "to": "one"}, {"from": "/input", "to": "two"},
            ]},
        ]
        for channel in ("send_goal", "get_result", "cancel_goal", "feedback", "status"):
            cases.append({"remaps": [{
                "from": f"work/_action/{channel}", "to": "split",
            }]})
            cases.append({"remaps": [{
                "from": "work", "to": f"split/_action/{channel}",
            }]})
        for options in cases:
            with self.subTest(options=options), self.assertRaises(aos.InvalidArgumentError):
                aos.Node("worker", context=self.context, **options)
        self.assertEqual(self.context.graph(timeout=0.1), [])
        with mock.patch.dict(os.environ, {"AOS_USE_SIM_TIME": "1"}):
            with self.assertRaises(aos.InvalidArgumentError):
                aos.Node("worker", context=self.context)
        node = self.node("valid")
        with self.assertRaises(aos.InvalidArgumentError):
            node.resolve_name("anything", kind="invalid")

    def test_whole_action_remap_drives_five_channels_and_result(self):
        interface = aos.Interface.load(ROOT / "interfaces/action/Fibonacci.json")
        provider = self.node("worker", "/original", launch_config={
            "name": "deployed", "namespace": "/runtime",
            "remaps": [{"kind": "action", "from": "~/job", "to": "/jobs/fibonacci"}],
        })
        caller = self.node("caller")

        def execute(goal):
            goal.succeed()
            return {"sequence": [0, 1, 1]}

        aos.ActionServer(provider, interface, "~/job", execute)
        client = aos.ActionClient(caller, interface, "/jobs/fibonacci")
        self.spin(provider, caller)
        self.assertTrue(client.wait_for_server(timeout_sec=2))
        goal = client.send_goal_async({"order": 3}).result(3)
        self.assertTrue(goal.accepted)
        self.assertEqual(goal.get_result_async(timeout=3).result(4), {
            "status": int(aos.GoalStatus.SUCCEEDED), "result": {"sequence": [0, 1, 1]},
        })
        expected = {"/jobs/fibonacci"} | {
            f"/jobs/fibonacci/_action/{channel}" for channel in
            ("send_goal", "get_result", "cancel_goal", "feedback", "status")
        }
        names = {entry["name"] for entry in provider.get_node_card()["endpoints"]}
        self.assertTrue(expected <= names, names)
        graph = self.context.graph(timeout=0.1)
        endpoints = {
            entry["endpoint"]["name"] for entry in graph
            if entry["node_name"] == "deployed" and entry["endpoint"]
        }
        self.assertTrue(expected <= endpoints, endpoints)
        self.assertFalse(any(name.startswith("/original/") for name in endpoints))


class QoSTest(PlatformTestCase):
    def test_depth_only_profile_defers_to_each_entity_default(self):
        from aos_sdk import _native
        from aos_sdk.qos import native_qos

        implicit = aos.QoSProfile(depth=6)
        explicit = aos.QoSProfile(preset="default", depth=6)
        self.assertIsNone(implicit.preset)
        self.assertNotEqual(implicit, explicit)
        self.assertEqual(native_qos(implicit), {"depth": 6})
        for preset, service in (("default", False), ("services_default", True),
                                ("action_status", False), ("parameters", True)):
            with self.subTest(preset=preset):
                resolved = _native._resolve_qos(
                    native_qos(implicit), default_preset=preset, service=service,
                )
                self.assertEqual(resolved, _native._resolve_qos(
                    6, default_preset=preset, service=service,
                ))
                self.assertEqual(resolved["sources"]["depth"], "override")
                self.assertEqual(resolved["sources"]["durability"], f"preset:{preset}")
        self.assertEqual(_native._resolve_qos(
            native_qos(explicit), default_preset="action_status",
        )["durability"], "volatile")

    def test_all_presets_and_override_sources_come_from_core(self):
        presets = {
            "default": (10, "reliable", "volatile"),
            "sensor_data": (5, "best_effort", "volatile"),
            "services_default": (10, "reliable", "volatile"),
            "action_status": (1, "reliable", "transient_local"),
            "parameters": (1000, "reliable", "volatile"),
            "parameter_events": (1000, "reliable", "volatile"),
            "lifecycle_services": (10, "reliable", "volatile"),
            "lifecycle_events": (10, "reliable", "volatile"),
            "clock": (1, "best_effort", "volatile"),
        }
        for preset, expected in presets.items():
            with self.subTest(preset=preset):
                qos = aos.QoSProfile(preset=preset)
                self.assertEqual((qos.depth, qos.reliability, qos.durability), expected)
                self.assertEqual(qos.history, "keep_last")
                self.assertTrue(all(
                    source == f"preset:{preset}"
                    for source in qos.effective_config["sources"].values()
                ))
        override = aos.QoSProfile(preset="sensor_data", depth=7)
        effective = override.effective_config
        self.assertEqual(effective["sources"]["depth"], "override")
        self.assertEqual(effective["sources"]["reliability"], "preset:sensor_data")
        effective["sources"]["depth"] = "cannot mutate core result"
        self.assertEqual(override.effective_config["sources"]["depth"], "override")

    def test_topic_qos_rejects_unknown_and_unsupported_values(self):
        node = self.node("qos_rejections")
        interface = aos.Interface.load(ROOT / "interfaces/String.json")
        before = node.get_node_card()["endpoints"]
        requests = [
            {"deadline": 1}, {"lifespan": 1}, {"liveliness": "automatic"},
            {"liveliness_lease_duration": 1}, {"history": "keep_all"},
            {"history": "system_default"}, {"reliability": "best_available"},
            {"durability": "unknown"}, {"depth": True}, {"depth": 0},
            {"depth": 1.5}, {"depth": None}, {"preset": "unknown"},
            False, 1.5, [], "unknown",
        ]
        for request in requests:
            with self.subTest(request=request), self.assertRaises(aos.InvalidArgumentError):
                node.create_publisher(interface, "invalid", qos_profile=request)
        self.assertEqual(node.get_node_card()["endpoints"], before)
        for request in requests[:13]:
            with self.subTest(profile=request), self.assertRaises(aos.InvalidArgumentError):
                aos.QoSProfile(**request)

    def test_topic_presets_reach_graph_and_bounded_receive_queue(self):
        node = self.node("topics")
        interface = aos.Interface.load(ROOT / "interfaces/String.json")
        publisher = node.create_publisher(
            interface, "samples", aos.QoSProfile(preset="sensor_data", depth=7),
        )
        subscriber = node.create_subscription(
            interface, "samples", qos_profile={"preset": "sensor_data", "depth": 2},
        )
        publisher.wait_for_subscribers(timeout=2)
        for sequence in range(5):
            publisher.publish({"data": str(sequence)})
        self.wait_for(lambda: subscriber.queue_stats["received_total"] == 5)
        stats = subscriber.queue_stats
        self.assertEqual((stats["depth"], stats["queued"], stats["dropped_total"]), (2, 2, 3))
        self.assertEqual(subscriber.recv(1), {"data": "3"})
        self.assertEqual(subscriber.recv(1), {"data": "4"})
        with self.assertRaises(aos.WaitTimeoutError):
            subscriber.recv(0)
        endpoints = [
            entry for entry in node.get_node_card()["endpoints"]
            if entry["name"] == "/samples"
        ]
        self.assertEqual(len(endpoints), 2)
        for entry in endpoints:
            self.assertEqual(entry["qos"]["preset"], "sensor_data")
            self.assertEqual(entry["qos"]["sources"]["depth"], "override")
            self.assertEqual(entry["qos"]["sources"]["reliability"], "preset:sensor_data")
        graph = [
            entry for entry in self.context.graph(timeout=0.1)
            if entry["endpoint"] and entry["endpoint"]["name"] == "/samples"
        ]
        self.assertEqual({entry["qos"]["depth"] for entry in graph}, {2, 7})
        self.assertTrue(all(entry["qos"]["reliability"] == "best_effort" for entry in graph))

    def test_service_profiles_reach_both_queue_owners_and_real_call(self):
        interface = aos.Interface.load(ROOT / "interfaces/AddTwoInts.json")
        provider, caller = self.node("service_provider"), self.node("service_caller")
        service = provider.create_service(
            interface, "add", lambda req: {"sum": req["a"] + req["b"]},
            qos_profile=aos.QoSProfile(depth=2),
        )
        client = caller.create_client(interface, "add", qos_profile=aos.QoSProfile(depth=3))
        self.spin(provider, caller)
        self.assertTrue(client.wait_for_service(timeout_sec=2))
        self.assertEqual(client.call_async({"a": 20, "b": 22}).result(3), {"sum": 42})
        self.assertEqual(service.queue_stats["depth"], 2)
        self.assertEqual(client.queue_stats["depth"], 3)
        self.assertEqual(service.queue_stats["received_total"], 1)
        self.assertEqual(client.queue_stats["received_total"], 1)
        for node, depth in ((provider, 2), (caller, 3)):
            qos = next(
                entry["qos"] for entry in node.get_node_card()["endpoints"]
                if entry["name"] == "/add"
            )
            self.assertEqual(qos["depth"], depth)
            self.assertEqual(qos["sources"]["depth"], "override")
            self.assertEqual(qos["sources"]["reliability"], "preset:services_default")
        for request in ("sensor_data", {"reliability": "best_effort"},
                        {"durability": "transient_local"}, {"history": "keep_all"}):
            with self.subTest(request=request):
                with self.assertRaises(aos.InvalidArgumentError):
                    caller.create_client(interface, "invalid", qos_profile=request)
                with self.assertRaises(aos.InvalidArgumentError):
                    provider.create_service(
                        interface, "invalid", lambda req: {"sum": 0}, qos_profile=request,
                    )

    def test_action_five_profiles_and_real_late_join_status(self):
        interface = aos.Interface.load(ROOT / "interfaces/action/Fibonacci.json")
        provider, caller = self.node("action_provider"), self.node("action_caller")
        profiles = {
            "goal_service_qos_profile": 2, "result_service_qos_profile": 3,
            "cancel_service_qos_profile": 4,
            "feedback_qos_profile": {"preset": "sensor_data", "depth": 5},
            "status_qos_profile": aos.QoSProfile(depth=6),
        }

        def execute(goal):
            goal.succeed()
            return {"sequence": [0, 1, 1]}

        aos.ActionServer(provider, interface, "job", execute, **profiles)
        client = aos.ActionClient(caller, interface, "job", **profiles)
        self.spin(provider, caller)
        self.assertTrue(client.wait_for_server(timeout_sec=2))
        goal = client.send_goal_async({"order": 3}).result(3)
        self.assertEqual(goal.get_result_async(timeout=3).result(4)["status"], 4)
        depths = {"send_goal": 2, "get_result": 3, "cancel_goal": 4, "feedback": 5, "status": 6}
        for node in (provider, caller):
            entries = {entry["name"]: entry for entry in node.get_node_card()["endpoints"]}
            for channel, depth in depths.items():
                qos = entries[f"/job/_action/{channel}"]["qos"]
                self.assertEqual(qos["depth"], depth)
                self.assertEqual(qos["sources"]["depth"], "override")
                expected = "sensor_data" if channel == "feedback" else (
                    "action_status" if channel == "status" else "services_default"
                )
                self.assertEqual(qos["preset"], expected)
        graph = self.context.graph(timeout=0.1)
        channels = [
            entry for entry in graph if entry["endpoint"]
            and entry["endpoint"]["name"].startswith("/job/_action/")
        ]
        self.assertEqual(len(channels), 10)
        for entry in channels:
            channel = entry["endpoint"]["name"].rsplit("/", 1)[-1]
            self.assertEqual(entry["qos"]["depth"], depths[channel])
            if channel == "status":
                self.assertEqual(entry["qos"]["durability"], "transient_local")

        # Construct only after the goal is terminal: a live status publication
        # cannot satisfy this check. No executor consumes this observer's queue.
        observer = aos.ActionClient(self.node("late_observer"), interface, "job", **profiles)
        deadline = time.monotonic() + 3
        observed = []
        while time.monotonic() < deadline:
            message = observer._native.recv_status(max(0, deadline - time.monotonic()))
            observed.extend(message["status_list"])
            if any(entry["goal_info"]["goal_id"] == goal.goal_id and entry["status"] == 4
                   for entry in observed):
                break
        self.assertTrue(any(
            entry["goal_info"]["goal_id"] == goal.goal_id and entry["status"] == 4
            for entry in observed
        ), observed)
        self.assertEqual(observer.get_result_async(goal.goal_id, timeout=3).result(4), {
            "status": 4, "result": {"sequence": [0, 1, 1]},
        })

    def test_action_volatile_status_does_not_replay_terminal_snapshot(self):
        interface = aos.Interface.load(ROOT / "interfaces/action/Fibonacci.json")
        provider, caller = self.node("volatile_provider"), self.node("volatile_caller")
        profile = aos.QoSProfile(preset="default", depth=6)

        def execute(goal):
            goal.succeed()
            return {"sequence": []}

        aos.ActionServer(provider, interface, "volatile", execute, status_qos_profile=profile)
        client = aos.ActionClient(caller, interface, "volatile", status_qos_profile=profile)
        self.spin(provider, caller)
        goal = client.send_goal_async({"order": 0}).result(3)
        self.assertEqual(goal.get_result_async(timeout=3).result(4)["status"], 4)
        observer = aos.ActionClient(
            self.node("volatile_observer"), interface, "volatile", status_qos_profile=profile,
        )
        status_qos = next(
            entry["qos"] for entry in provider.get_node_card()["endpoints"]
            if entry["name"] == "/volatile/_action/status"
        )
        self.assertEqual((status_qos["preset"], status_qos["durability"]), ("default", "volatile"))
        with self.assertRaises(aos.WaitTimeoutError):
            observer._native.recv_status(0.1)
        self.assertEqual(observer.get_result_async(goal.goal_id, timeout=3).result(4)["status"], 4)

    def test_action_profiles_reject_unsupported_policies_before_allocation(self):
        node = self.node("invalid_action")
        interface = aos.Interface.load(ROOT / "interfaces/action/Fibonacci.json")
        before = node.get_node_card()["endpoints"]
        invalid = [
            {f"{channel}_qos_profile": {"reliability": "best_effort"}}
            for channel in ("goal_service", "result_service", "cancel_service")
        ] + [
            {"feedback_qos_profile": {"history": "keep_all"}},
            {"status_qos_profile": {"deadline": 1}},
            {"status_qos_profile": "unknown"},
        ]
        for profiles in invalid:
            with self.subTest(profiles=profiles):
                with self.assertRaises(aos.InvalidArgumentError):
                    aos.ActionServer(node, interface, "invalid", lambda goal: {}, **profiles)
                with self.assertRaises(aos.InvalidArgumentError):
                    aos.ActionClient(node, interface, "invalid", **profiles)
        self.assertEqual(node.get_node_card()["endpoints"], before)


class ServiceFailureTest(PlatformTestCase):
    def test_remote_error_text_alone_is_not_a_queue_overflow(self):
        interface = aos.Interface.load(ROOT / "interfaces/AddTwoInts.json")
        provider, caller = self.node("remote_error"), self.node("remote_error_client")
        service = provider.create_service(interface, "business", lambda request: {"sum": 3})
        client = caller.create_client(interface, "business")
        self.assertTrue(client.wait_for_service(timeout_sec=2))
        result = client.call_async({"a": 1, "b": 2}, timeout=2)
        request = service._native.recv(1)
        request.reply_error("SERVICE_QUEUE_OVERFLOW")
        with self.assertRaises(aos.ServiceError) as remote:
            result.result(2)
        self.assertIn("SERVICE_QUEUE_OVERFLOW", str(remote.exception))
        self.assertNotIsInstance(remote.exception, aos.QueueOverflowError)

    def test_no_response_is_distinct_from_a_real_deadline(self):
        node = self.node("failure_client")
        interface = aos.Interface.load(ROOT / "interfaces/AddTwoInts.json")
        missing = node.create_client(interface, "no_provider")
        with self.assertRaises(aos.NoResponseError) as no_response:
            missing.call({"a": 1, "b": 2}, timeout=2)
        self.assertIsInstance(no_response.exception, aos.CommunicationError)
        self.assertNotIsInstance(no_response.exception, aos.WaitTimeoutError)

        provider = self.node("not_spinning")
        provider.create_service(interface, "waiting", lambda request: {"sum": 3})
        waiting = node.create_client(interface, "waiting")
        self.assertTrue(waiting.wait_for_service(timeout_sec=2))
        with self.assertRaises(aos.WaitTimeoutError) as timeout:
            waiting.call({"a": 1, "b": 2}, timeout=0.03)
        self.assertNotIsInstance(timeout.exception, aos.NoResponseError)

    def test_server_queue_eviction_reaches_python_as_queue_overflow(self):
        interface = aos.Interface.load(ROOT / "interfaces/AddTwoInts.json")
        provider, caller = self.node("overflow_provider"), self.node("overflow_caller")
        executed = []

        def handle(request):
            executed.append(request)
            return {"sum": request["a"] + request["b"]}

        service = provider.create_service(interface, "bounded", handle, qos_profile=1)
        client = caller.create_client(interface, "bounded")
        self.assertTrue(client.wait_for_service(timeout_sec=2))
        first = client.call_async({"a": 1, "b": 1}, timeout=3)
        self.wait_for(lambda: service.queue_stats["received_total"] == 1)
        second = client.call_async({"a": 2, "b": 2}, timeout=3)
        self.wait_for(lambda: service.queue_stats["dropped_total"] == 1)
        with self.assertRaises(aos.QueueOverflowError) as overflow:
            first.result(2)
        self.assertIsInstance(overflow.exception, aos.CommunicationError)
        self.assertNotIsInstance(overflow.exception, aos.WaitTimeoutError)
        self.assertEqual(executed, [])
        self.spin(provider)
        self.assertEqual(second.result(2), {"sum": 4})
        self.assertEqual(executed, [{"a": 2, "b": 2}])


class TimeTest(PlatformTestCase):
    def simulated(self, name="timed"):
        node = self.node(name, use_sim_time=True)
        self.assertFalse(node.clock.now().initialized)
        node.clock.update_ros_time(0)
        return node

    def single_executor(self, node):
        executor = aos.SingleThreadedExecutor(context=self.context)
        executor.add_node(node)
        self.addCleanup(executor.shutdown)
        return executor

    def sleeping(self, rate):
        entered, done = threading.Event(), threading.Event()
        errors = []

        def sleep():
            entered.set()
            try:
                rate.sleep()
            except Exception as error:
                errors.append(error)
            finally:
                done.set()

        thread = threading.Thread(target=sleep, name="platform-rate")
        thread.start()
        self.assertTrue(entered.wait(2))

        def stop():
            try:
                rate.destroy()
            except aos.InvalidStateError:
                pass
            thread.join(2)
            self.assertFalse(thread.is_alive())

        self.addCleanup(stop)
        return done, errors

    def test_clock_domains_zero_initialization_and_strict_time_values(self):
        node = self.node("clock_defaults")
        self.assertIs(node.get_clock(), node.clock)
        now = node.clock.now()
        self.assertEqual(now.clock_type, aos.ClockType.ROS)
        self.assertTrue(now.initialized)
        self.assertFalse(now.use_sim_time)
        self.assertGreater(now.nanoseconds, 0)
        self.assertEqual(sum((now.seconds_nanoseconds()[0] * 1_000_000_000,
                              now.seconds_nanoseconds()[1])), now.nanoseconds)
        for kind in (aos.ClockType.STEADY, aos.ClockType.SYSTEM):
            with aos.Clock(kind, context=self.context) as clock:
                self.assertEqual(clock.clock_type, kind)
                self.assertTrue(clock.now().initialized)
                with self.assertRaises(aos.InvalidArgumentError):
                    clock.set_use_sim_time(True)
        with aos.Clock("ros", context=self.context) as clock:
            clock.set_use_sim_time(True)
            self.assertFalse(clock.now().initialized)
            clock.update_ros_time(0)
            self.assertTrue(clock.now().initialized)
            self.assertEqual(clock.now().nanoseconds, 0)
            for value in (True, 1.5, -1, 2**63):
                with self.subTest(nanoseconds=value), self.assertRaises(aos.InvalidArgumentError):
                    clock.update_ros_time(value)
        for period in (True, -1, float("nan"), float("inf"), 1e30):
            with self.subTest(period=period), self.assertRaises(aos.InvalidArgumentError):
                node.create_timer(period, lambda: None)
        for frequency in (True, 0, -1, float("nan"), float("inf"), 1e30, 1e-30):
            with self.subTest(frequency=frequency), self.assertRaises(aos.InvalidArgumentError):
                node.create_rate(frequency)
        with self.assertRaises(aos.InvalidArgumentError):
            node.set_use_sim_time(1)
        with self.assertRaises(aos.InvalidArgumentError):
            node.create_timer(1, lambda one, two: None)
        with self.assertRaises(aos.InvalidArgumentError):
            node.create_timer(1, lambda: None, autostart=1)

    def test_real_clock_topic_controls_pause_forward_and_backward_timer(self):
        node = self.node("clock_consumer", use_sim_time=True, remaps=[
            {"kind": "topic", "from": "/clock", "to": "/simulation/clock"},
        ])
        source = self.node("clock_source")
        publisher = source.create_publisher(
            aos.clock_interface(), "/simulation/clock", qos_profile="clock",
        )
        publisher.wait_for_subscribers(timeout=2)
        calls = []
        timer = node.create_timer(1.0, calls.append)
        executor = self.single_executor(node)
        self.assertFalse(node.clock.now().initialized)
        self.assertIsNone(timer.time_until_next_call())
        executor.spin_once(timeout_sec=0.01)
        self.assertEqual(calls, [])

        def publish(nanoseconds):
            seconds, nanos = divmod(nanoseconds, 1_000_000_000)
            publisher.publish({"clock": {"sec": seconds, "nanosec": nanos}})
            self.wait_for(lambda: node.clock.now().initialized
                          and node.clock.now().nanoseconds == nanoseconds)

        publish(0)
        self.assertEqual(timer.time_until_next_call(), 1_000_000_000)
        executor.spin_once(timeout_sec=0)
        self.assertEqual(calls, [])
        publish(2_000_000_000)
        executor.spin_once(timeout_sec=0)
        self.assertEqual(calls, [aos.TimerInfo(1_000_000_000, 2_000_000_000)])
        executor.spin_once(timeout_sec=0)
        publish(2_000_000_000)  # Paused replay does not start a new period.
        executor.spin_once(timeout_sec=0.01)
        self.assertEqual(len(calls), 1)
        publish(100_000_000_000)
        executor.spin_once(timeout_sec=0)
        self.assertEqual(len(calls), 2)
        executor.spin_once(timeout_sec=0)
        self.assertEqual(len(calls), 2)
        publish(5_000_000_000)
        self.assertEqual(timer.time_until_next_call(), 1_000_000_000)
        publish(5_900_000_000)
        executor.spin_once(timeout_sec=0)
        self.assertEqual(len(calls), 2)
        publish(6_000_000_000)
        executor.spin_once(timeout_sec=0)
        self.assertEqual(calls[-1], aos.TimerInfo(6_000_000_000, 6_000_000_000))
        reasons = {event["reason"] for event in node.clock.take_jump_events()}
        self.assertTrue({"initialized", "forward", "backward"} <= reasons)
        self.assertEqual(node.clock.take_jump_events(), [])
        self.assertEqual(node.clock.forward_update_count, 4)
        clock_endpoint = next(
            entry for entry in node.get_node_card()["endpoints"]
            if entry["name"] == "/simulation/clock"
        )
        self.assertEqual(clock_endpoint["qos"]["preset"], "clock")

    def test_node_clock_activation_opens_subscription_and_service_stays_live(self):
        node = self.node("activate")
        node.clock.set_use_sim_time(True)
        self.assertFalse(node.clock.now().initialized)
        self.assertIn("/clock", {entry["name"] for entry in node.get_node_card()["endpoints"]})
        interface = aos.Interface.load(ROOT / "interfaces/AddTwoInts.json")
        node.create_service(interface, "add", lambda req: {"sum": req["a"] + req["b"]})
        client = node.create_client(interface, "add")
        self.spin(node)
        self.assertEqual(client.call_async({"a": 2, "b": 3}).result(2), {"sum": 5})
        absent = node.create_client(interface, "absent")
        started = time.monotonic()
        self.assertFalse(absent.wait_for_service(timeout_sec=0.03))
        self.assertLess(time.monotonic() - started, 1)
        self.assertFalse(node.clock.now().initialized)
        node.set_use_sim_time(False)
        self.assertTrue(node.clock.now().initialized)
        self.assertFalse(node.clock.now().use_sim_time)
        reasons = [event["reason"] for event in node.clock.take_jump_events()]
        self.assertEqual(reasons, ["ros_time_activated", "ros_time_deactivated"])

    def test_forward_clock_diagnostics_coalesce_without_losing_transitions(self):
        with aos.Clock("ros", context=self.context) as clock:
            clock.set_use_sim_time(True)
            clock.update_ros_time(0)
            clock.update_ros_time(10)
            clock.update_ros_time(5)
            for value in range(6, 2001):
                clock.update_ros_time(value)
            events = clock.take_jump_events()
            self.assertEqual([event["reason"] for event in events], [
                "ros_time_activated", "initialized", "forward", "backward", "forward",
            ])
            self.assertEqual(
                (events[2]["before"], events[2]["after"], events[2]["delta"]),
                (0, 10, 10),
            )
            self.assertEqual(
                (events[4]["before"], events[4]["after"], events[4]["delta"]),
                (5, 2000, 1995),
            )
            self.assertEqual(events[4]["sequence"], 5)
            self.assertEqual(clock.forward_update_count, 1996)
            self.assertEqual(clock.take_jump_events(), [])
            clock.update_ros_time(2000)
            self.assertEqual(clock.forward_update_count, 1996)
            self.assertEqual(clock.take_jump_events(), [])

    def test_reentrant_group_never_overlaps_one_timer_and_forward_jump_coalesces(self):
        node = self.simulated()
        started, release = threading.Event(), threading.Event()
        lock = threading.Lock()
        state = {"active": 0, "max_active": 0, "calls": 0}

        def callback():
            with lock:
                state["active"] += 1
                state["max_active"] = max(state["max_active"], state["active"])
                state["calls"] += 1
            started.set()
            try:
                release.wait(3)
            finally:
                with lock:
                    state["active"] -= 1

        node.create_timer(1.0, callback, callback_group=aos.ReentrantCallbackGroup())
        self.spin(node)
        self.addCleanup(release.set)
        node.clock.update_ros_time(1_000_000_000)
        self.assertTrue(started.wait(2))
        node.clock.update_ros_time(100_000_000_000)
        time.sleep(0.03)
        self.assertEqual(state["calls"], 1)
        self.assertEqual(state["max_active"], 1)
        release.set()
        # The overdue next period stays ready throughout the first callback.
        # Finishing its lease permits one immediate catch-up at paused time.
        self.wait_for(lambda: state["calls"] == 2 and state["active"] == 0)
        time.sleep(0.03)
        self.assertEqual(state["calls"], 2)
        node.clock.update_ros_time(101_000_000_000)
        self.wait_for(lambda: state["calls"] == 3)
        self.assertEqual(state["max_active"], 1)

    def test_cancellation_reset_backward_jump_and_close_invalidate_queued_callbacks(self):
        node = self.simulated()
        calls = []
        timer = node.create_timer(1, lambda: calls.append("called"), autostart=False)
        self.assertTrue(timer.is_canceled())
        node.clock.update_ros_time(1_000_000_000)
        self.assertFalse(timer.is_ready())
        timer.reset()
        node.clock.update_ros_time(2_000_000_000)
        pending = timer._take_callback()
        timer.cancel()
        pending()
        self.assertEqual(calls, [])
        timer.reset()
        node.clock.update_ros_time(3_000_000_000)
        pending = timer._take_callback()
        node.clock.update_ros_time(0)
        pending()
        self.assertEqual(calls, [])
        node.clock.update_ros_time(1_000_000_000)
        timer._take_callback()()
        self.assertEqual(calls, ["called"])
        node.clock.update_ros_time(2_000_000_000)
        pending = timer._take_callback()
        timer.destroy()
        pending()
        self.assertEqual(calls, ["called"])

    def test_callback_exception_releases_native_timer_lease(self):
        node = self.simulated()
        calls = []

        def callback():
            calls.append(len(calls))
            if len(calls) == 1:
                raise RuntimeError("intentional timer error")

        node.create_timer(1, callback)
        executor = self.single_executor(node)
        node.clock.update_ros_time(1_000_000_000)
        with self.assertRaisesRegex(RuntimeError, "intentional timer error"):
            executor.spin_once(timeout_sec=0)
        node.clock.update_ros_time(2_000_000_000)
        executor.spin_once(timeout_sec=0)
        self.assertEqual(calls, [0, 1])

    def test_executor_submission_failure_releases_native_timer_lease(self):
        node = self.simulated()
        called = threading.Event()
        timer = node.create_timer(1, called.set)
        executor = aos.MultiThreadedExecutor(2, context=self.context)
        executor.add_node(node)
        self.addCleanup(executor.shutdown)
        node.clock.update_ros_time(1_000_000_000)
        with mock.patch.object(executor._pool, "submit", side_effect=RuntimeError("submit failed")):
            with self.assertRaisesRegex(RuntimeError, "submit failed"):
                executor.spin_once(timeout_sec=0)
        node.clock.update_ros_time(2_000_000_000)
        self.assertTrue(timer.is_ready())
        executor.spin_once(timeout_sec=0)
        self.assertTrue(called.wait(2))

    def test_rate_sleep_uses_real_clock_samples_without_executor_and_shutdown_wakes(self):
        node = self.node("rate_consumer", use_sim_time=True)
        source = self.node("rate_source")
        publisher = source.create_publisher(aos.clock_interface(), "/clock", qos_profile="clock")
        publisher.wait_for_subscribers(timeout=2)
        rate = node.create_rate(1)
        done, errors = self.sleeping(rate)
        self.assertFalse(done.wait(0.02))
        publisher.publish({"clock": {"sec": 0, "nanosec": 0}})
        self.wait_for(lambda: node.clock.now().initialized)
        self.assertFalse(done.wait(0.02))
        publisher.publish({"clock": {"sec": 1, "nanosec": 0}})
        self.assertTrue(done.wait(2))
        self.assertEqual(errors, [])
        done, errors = self.sleeping(rate)
        self.assertFalse(done.wait(0.02))
        self.context.shutdown()
        self.assertTrue(done.wait(2))
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], aos.InvalidStateError)

    def test_node_shutdown_wakes_rates_on_all_clock_domains(self):
        for kind in (aos.ClockType.STEADY, aos.ClockType.SYSTEM, aos.ClockType.ROS):
            with self.subTest(kind=kind):
                node = self.node(f"rate_{kind.value}")
                clock = aos.Clock(kind, context=self.context)
                rate = node.create_rate(0.001, clock=clock)
                done, errors = self.sleeping(rate)
                self.assertFalse(done.wait(0.02))
                node.destroy_node()
                self.assertTrue(done.wait(2))
                self.assertEqual(len(errors), 1)
                self.assertIsInstance(errors[0], aos.InvalidStateError)
                # Node owns its Timer/Rate, but does not close the shared clock.
                self.assertTrue(clock.now().initialized)
        self.context.shutdown()
        with self.assertRaises(aos.InvalidStateError):
            clock.now()


if __name__ == "__main__":
    unittest.main()
