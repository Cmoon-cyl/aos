"""Installed Python/CLI security bindings against the shipped router entry."""
import copy
from process_support import console_process_options, interrupt_process, read_line

import json
import os
from pathlib import Path
import queue
import signal
import socket
import subprocess
import sys
import tempfile
import time
import threading
import unittest
from unittest import mock

import aos_sdk as aos
from test_platform_foundation import ROOT
from process_support import console_process_options, interrupt_process, read_line


class SecurityBindingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory(prefix="aos-security-bindings-")
        cls.directory = Path(cls.temporary.name)
        os.chmod(cls.directory, 0o700)
        cls.binary = Path(os.environ["AOS_CLI"]).resolve()

        def openssl(*args):
            result = subprocess.run(
                ["openssl", *args], cwd=cls.directory, capture_output=True, text=True,
                timeout=15,
            )
            if result.returncode:
                raise AssertionError("OpenSSL fixture failed: " + result.stderr)

        openssl(
            "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1",
            "-nodes", "-sha256", "-days", "2", "-subj", "/CN=AOS binding test CA",
            "-keyout", "ca.key", "-out", "ca.pem",
            "-addext", "basicConstraints=critical,CA:TRUE",
            "-addext", "keyUsage=critical,keyCertSign,cRLSign",
        )
        for serial, name in enumerate(("server", "provider", "operator"), start=1):
            openssl(
                "req", "-new", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1",
                "-nodes", "-subj", f"/CN={name}", "-keyout", f"{name}.key",
                "-out", f"{name}.csr",
            )
            extension = cls.directory / f"{name}.ext"
            extension.write_text(
                "basicConstraints=critical,CA:FALSE\n"
                "keyUsage=critical,digitalSignature,keyAgreement\n"
                "extendedKeyUsage=serverAuth,clientAuth\n"
                "subjectAltName=IP:127.0.0.1,DNS:localhost\n",
            )
            openssl(
                "x509", "-req", "-sha256", "-days", "2", "-in", f"{name}.csr",
                "-CA", "ca.pem", "-CAkey", "ca.key", "-set_serial", str(serial),
                "-extfile", extension.name, "-out", f"{name}.pem",
            )

    @classmethod
    def tearDownClass(cls):
        cls.temporary.cleanup()

    def setUp(self):
        self.environment = {key: value for key, value in os.environ.items()
                            if not key.startswith("AOS_")}
        patch = mock.patch.dict(os.environ, self.environment, clear=True)
        patch.start()
        self.addCleanup(patch.stop)
        temporary = tempfile.TemporaryDirectory(dir=self.directory, prefix="case-")
        self.addCleanup(temporary.cleanup)
        self.case = Path(temporary.name)
        self.interface = aos.Interface.load(ROOT / "interfaces/String.json")
        self.provider = {"name": "provider", "namespace": "/secure"}
        self.operator = {"name": "operator", "namespace": "/secure"}
        self.policy = {"schema_version": 1, "domain_id": 29, "subjects": [
            {"id": "provider", "common_name": "provider", "nodes": [self.provider],
             "endpoints": [{
                 "node": self.provider, "namespace": "/secure", "name": "/secure/data",
                 "interface": self.interface.definition, "operations": ["publish"],
             }]},
            {"id": "operator", "common_name": "operator", "nodes": [self.operator],
             "graph_nodes": [self.provider],
             "systems": [{"node": self.operator, "target": self.provider,
                          "operations": ["describe"]}],
             "endpoints": [{
                 "node": self.operator, "namespace": "/secure", "name": "/secure/data",
                 "interface": self.interface.definition, "operations": ["subscribe"],
             }]},
        ]}

    def tls_files(self, name):
        return {
            "schema_version": 1, "root_ca_certificate": str(self.directory / "ca.pem"),
            "certificate": str(self.directory / f"{name}.pem"),
            "private_key": str(self.directory / f"{name}.key"),
        }

    def client(self, name, endpoint):
        return self.tls_files(name) | {
            "domain_id": 29, "router_endpoints": [endpoint], "connect_timeout_ms": 500,
        }

    def cli(self, group, verb, request, *, environment=None, code=0):
        result = subprocess.run(
            [str(self.binary), group, verb, "--request", "-"],
            input=json.dumps(request), capture_output=True, text=True,
            env=environment or self.environment, timeout=8,
        )
        self.assertEqual(result.returncode, code, result.stdout + result.stderr)
        return json.loads(result.stdout)

    def start_router(self):
        with socket.socket() as reserved:
            reserved.bind(("127.0.0.1", 0))
            port = reserved.getsockname()[1]
        endpoint = f"tls/127.0.0.1:{port}"
        request = {
            "router": self.tls_files("server") | {"listen_endpoints": [endpoint]},
            "policy": self.policy, "startup_timeout": 5, "shutdown_timeout": 1,
        }
        path = self.case / "router.json"
        path.write_text(json.dumps(request))
        process = subprocess.Popen(
            [str(self.binary), "security", "serve", "--request", str(path)],
            env=self.environment | {
                "AOS_SECURITY_PROFILE": "/ignored/router/client-profile",
                "AOS_ZENOH_CONFIG": "/ignored/router/native-override",
            },
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, encoding="utf-8", **console_process_options(),
        )

        def cleanup():
            if process.poll() is None:
                interrupt_process(process, signal.SIGTERM)
            try:
                process.communicate(timeout=4)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate(timeout=3)
            process.stdout.close()
            process.stderr.close()

        self.addCleanup(cleanup)
        line = read_line(process.stdout, 6)
        if not line:
            self.fail("router exited before ready: " + process.stderr.read())
        ready = json.loads(line)
        self.assertEqual(ready["event"], "router_ready", ready)
        self.assertTrue(ready["data"]["zid"])
        self.assertTrue(all(value.startswith("tls/") for value in ready["data"]["locators"]))
        self.assertEqual(ready["data"]["domain_id"], 29)
        self.assertEqual(
            ready["data"]["policy_digest"], aos.compile_security_policy(self.policy)["source_digest"],
        )
        profiles = {}
        for name in ("provider", "operator"):
            profile = self.case / f"{name}.json"
            profile.write_text(json.dumps(self.client(name, endpoint)))
            profiles[name] = profile
        return process, profiles

    def context(self, profile):
        context = aos.Context(security_profile=profile)
        self.addCleanup(context.__exit__, None, None, None)
        self.assertEqual(context.effective_config["domain_id"]["value"], 29)
        return context

    def install_entry(self, source):
        prefix = self.case / "installed"
        (prefix / "share/secure_demo").mkdir(parents=True)
        entry = prefix / "lib/secure_demo/provider"
        entry.parent.mkdir(parents=True)
        entry.write_text("#!" + sys.executable + "\n" + source)
        entry.chmod(0o755)
        index = prefix / "share/aos/resource_index/packages"
        index.mkdir(parents=True)
        (index / "secure_demo.json").write_text(json.dumps({
            "schema_version": 1, "name": "secure_demo", "version": "1.0.0",
            "runtime_abi": "aos.process.v1",
            "entries": {"provider": {
                "executable": "lib/secure_demo/provider", "kind": "aos_node",
                **({"interpreter": sys.executable, "interpreter_args": ["-u"]} if os.name == "nt" else {}),
            }}, "resources": {},
        }))
        return prefix

    def test_federated_and_local_router_configs_match_python_and_cli(self):
        profile = self.tls_files("server") | {
            "listen_endpoints": ["tls/127.0.0.1:7447"], "local_peers": True,
        }
        local = aos.router_security_config(profile, self.policy)
        response = self.cli("security", "router-config", {
            "router": profile, "policy": self.policy,
        })["data"]
        self.assertEqual(response["config"], local)
        self.assertTrue(local["scouting"]["gossip"]["enabled"])
        self.assertEqual(response["compiled"], aos.compile_security_policy(self.policy))
        profile["federation"] = {
            "connect_endpoints": ["tls/127.0.0.1:7448"],
            "relays": [{"id": "relay", "common_name": "relay",
                        "forward_subjects": ["provider", "operator"]}],
        }
        response = self.cli("security", "router-config", {
            "router": profile, "policy": self.policy,
        })["data"]
        # Python returns only native config. CLI additionally returns the
        # effective compiled policy, whose ACL must equal that same config.
        self.assertEqual(response["config"], aos.router_security_config(profile, self.policy))
        compiled_acl = response["compiled"]["access_control"]
        native_acl = copy.deepcopy(response["config"]["access_control"])
        by_id = {subject["id"]: subject for subject in compiled_acl["subjects"]}
        for subject in native_acl["subjects"]:
            # Native Config serializes absent optional identity filters as
            # null. Only those exact unused fields may differ from compiler JSON.
            extra = set(subject) - set(by_id[subject["id"]])
            self.assertLessEqual(extra, {"interfaces", "usernames", "zids"})
            for name in extra:
                self.assertIsNone(subject.pop(name))
        self.assertEqual(compiled_acl, native_acl)
        self.assertNotEqual(response["compiled"]["source_digest"],
                            aos.compile_security_policy(self.policy)["source_digest"])
        self.assertTrue(any(grant["operation"].startswith("relay.")
                            for grant in response["compiled"]["expanded_grants"]))

    def test_offline_python_and_cli_share_the_compiler_and_strict_profiles(self):
        compiled = aos.compile_security_policy(self.policy)
        response = self.cli("security", "compile", {"policy": self.policy}, environment={
            **self.environment, "AOS_SECURITY_PROFILE": "",
        })
        self.assertFalse(response["data"]["network_opened"])
        self.assertEqual(response["data"]["compiled"], compiled)
        self.assertEqual(compiled["access_control"]["default_permission"], "deny")
        profile = self.client("operator", "tls/127.0.0.1:7447")
        native = aos.client_security_config(profile)
        self.assertEqual(
            self.cli("security", "client-config", {"client": profile})["data"]["config"], native,
        )
        self.assertEqual(native["transport"]["link"]["protocols"], ["tls"])
        for field, bad in (("router_endpoints", ["tcp/127.0.0.1:7447"]),
                           ("private_key", "relative.key")):
            with self.subTest(field=field):
                with self.assertRaises(aos.InvalidArgumentError):
                    aos.client_security_config(profile | {field: bad})
        invalid = copy.deepcopy(self.policy)
        invalid["subjects"][0]["endpoints"][0]["name"] = "/secure/**"
        with self.assertRaises(aos.InvalidArgumentError):
            aos.compile_security_policy(invalid)
        self.assertFalse(self.cli("security", "compile", {"policy": invalid}, code=2)["ok"])

    def test_invalid_security_selection_never_falls_back_to_a_working_peer(self):
        peer = self.case / "peer.json5"
        peer.write_text('{"mode":"peer","listen":{"endpoints":[]},'
                        '"scouting":{"multicast":{"enabled":false},"gossip":{"enabled":false}}}')
        for value in ("", "relative.json", str(self.case / "missing.json")):
            environment = self.environment | {
                "AOS_SECURITY_PROFILE": value, "AOS_ZENOH_CONFIG": str(peer),
            }
            with self.subTest(profile=value), mock.patch.dict(os.environ, environment, clear=True):
                with self.assertRaises(aos.InvalidArgumentError):
                    aos.Context()
                with self.assertRaises(aos.InvalidArgumentError):
                    aos.Context(security_profile=value)
                report = aos.doctor_report(timeout=3)
                self.assertEqual(report["status"], "error", report)
                self.assertEqual(report["context"]["status"], "error", report)
                self.assertEqual(report["graph"]["status"], "not_run", report)
                self.assertEqual(report["security"]["authentication"], "unknown")
                response = self.cli("config", "resolve", {}, environment=environment, code=2)
                self.assertEqual(response["error"]["phase"], "configuration")

    def test_python_empty_security_environment_cannot_fall_back(self):
        peer = self.case / "peer.json5"
        peer.write_text('{"mode":"peer","listen":{"endpoints":[]},'
                        '"scouting":{"multicast":{"enabled":false},"gossip":{"enabled":false}}}')
        with mock.patch.dict(os.environ, {"AOS_ZENOH_CONFIG": str(peer)}):
            # Reproduce the Windows distinction on every host: Python retains
            # the empty selection while the native environment has no entry.
            self.assertNotIn("AOS_SECURITY_PROFILE", os.environ)
            with mock.patch("os.environ", dict(os.environ, AOS_SECURITY_PROFILE="")):
                with self.assertRaises(aos.InvalidArgumentError):
                    aos.Context()

    def test_empty_context_environment_keeps_profile_doctor_for_sdk(self):
        self.check_empty_context_environment("sdk")

    def test_python_environment_profile_opens_the_tls_session(self):
        self.check_environment_profile(native_only=False)

    def test_native_only_environment_profile_opens_the_tls_session(self):
        self.check_environment_profile(native_only=True)

    def check_environment_profile(self, native_only):
        _, profiles = self.start_router()
        provider_context = self.context(profiles["provider"])
        provider = aos.Node("provider", "/secure", context=provider_context)
        self.addCleanup(provider.__exit__, None, None, None)
        publisher = provider.create_publisher(self.interface, "/secure/data")
        key, value = "AOS_SECURITY_PROFILE", str(profiles["operator"])
        self.assertNotIn(key, os.environ)
        if native_only:
            os.putenv(key, value)
            self.addCleanup(os.unsetenv, key)
            self.assertNotIn(key, os.environ)
        else:
            os.environ[key] = value
        consumer_context = aos.Context()
        self.addCleanup(consumer_context.__exit__, None, None, None)
        self.assertEqual(consumer_context.effective_config["domain_id"]["value"], 29)
        operator = aos.Node("operator", "/secure", context=consumer_context)
        self.addCleanup(operator.__exit__, None, None, None)
        subscription = operator.create_subscription(self.interface, "/secure/data")
        deadline = time.monotonic() + 3
        while True:
            publisher.publish({"data": "environment-selected TLS"})
            try:
                self.assertEqual(subscription.recv(0.1), {"data": "environment-selected TLS"})
                break
            except aos.WaitTimeoutError:
                self.assertLess(time.monotonic(), deadline)
        if native_only:
            os.putenv("AOS_PROCESS_CONFIG", "/invalid/native-only-bootstrap")
            self.addCleanup(os.unsetenv, "AOS_PROCESS_CONFIG")
        report = aos.doctor_report(timeout=3)
        self.assertEqual(report["status"], "ok", report)
        self.assertEqual(report["domain"]["value"], 29, report)
        self.assertEqual(report["config"]["source"], "security_profile", report)
        self.assertEqual(report["security"]["encryption"], "TLS-only validated configuration")
        self.assertEqual(report["shutdown"]["status"], "ok")
        self.assertEqual(report["deployment"]["process_config_present"], native_only)

    def test_native_only_environment_profile_keeps_doctor_selection(self):
        _, profiles = self.start_router()
        self.assertNotIn("AOS_SECURITY_PROFILE", os.environ)
        os.putenv("AOS_SECURITY_PROFILE", str(profiles["operator"]))
        self.addCleanup(os.unsetenv, "AOS_SECURITY_PROFILE")
        report = aos.doctor_report(timeout=3)
        self.assertEqual(report["status"], "ok", report)
        self.assertEqual(report["domain"]["value"], 29, report)
        self.assertEqual(report["security"]["encryption"], "TLS-only validated configuration")

    def test_conflicting_environment_profiles_fail_closed_without_echoing_values(self):
        python_path, native_path = "/private/python-profile", "/private/native-profile"
        with mock.patch.dict(os.environ, {"AOS_SECURITY_PROFILE": python_path}):
            os.putenv("AOS_SECURITY_PROFILE", native_path)
            with self.assertRaises(aos.InvalidArgumentError) as caught:
                aos.Context()
            self.assertNotIn(python_path, str(caught.exception))
            self.assertNotIn(native_path, str(caught.exception))
            report = aos.doctor_report(timeout=3)
            self.assertEqual(report["status"], "error", report)
            self.assertEqual(report["context"]["status"], "error", report)
            self.assertEqual(report["graph"]["status"], "not_run", report)
            self.assertFalse(report["security"]["encryption"] == "TLS-only validated configuration")
            self.assertNotIn(python_path, json.dumps(report))
            self.assertNotIn(native_path, json.dumps(report))

    def test_invalid_python_environment_type_keeps_invalid_argument_error(self):
        with mock.patch("os.environ", dict(os.environ, AOS_SECURITY_PROFILE=object())):
            with self.assertRaises(aos.InvalidArgumentError):
                aos.Context()

    def test_cli_empty_context_environment_keeps_profile_doctor(self):
        self.check_empty_context_environment("cli")

    def check_empty_context_environment(self, entry):
        _, profiles = self.start_router()
        environment = self.environment | {"AOS_ZENOH_CONFIG": "", "AOS_DOMAIN_ID": ""}
        with self.subTest(entry=entry), mock.patch.dict(os.environ, environment, clear=True):
            if entry == "sdk":
                report = aos.doctor_report(security_profile=profiles["operator"], timeout=3)
            else:
                response = self.cli("doctor", "report", {
                    "security_profile": str(profiles["operator"]),
                    "python": sys.executable, "timeout": 3,
                }, environment=environment)
                selected = response["data"]["python_native_module"]
                self.assertEqual(selected["status"], "ok", selected)
                report = selected["report"]
            self.assertEqual(report["status"], "ok", report)
            self.assertEqual(report["config"]["source"], "security_profile")
            self.assertIsNone(report["config"]["path"])
            self.assertEqual(report["domain"]["value"], 29)
            self.assertEqual(report["domain"]["source"],
                             "security_profile" if entry == "sdk" else "argument")
            self.assertIn("not independently observed", report["security"]["authentication"])
            self.assertEqual(report["security"]["authorization"], "unknown")
            self.assertEqual(report["shutdown"]["status"], "ok")
            self.assertEqual(os.environ["AOS_ZENOH_CONFIG"], "")
            self.assertEqual(os.environ["AOS_DOMAIN_ID"], "")

    def test_installed_router_python_data_and_explicit_profile_precedence(self):
        router, profiles = self.start_router()
        with mock.patch.dict(os.environ, {"AOS_SECURITY_PROFILE": "/missing/environment-profile"}):
            os.putenv("AOS_SECURITY_PROFILE", "/different/native-profile")
            producer_context = self.context(profiles["provider"])
            consumer_context = self.context(profiles["operator"])
            report = aos.doctor_report(security_profile=profiles["operator"], timeout=3)
            self.assertEqual(report["status"], "ok", report)
            self.assertEqual(report["domain"]["value"], 29)
            self.assertEqual(report["domain"]["source"], "security_profile")
            self.assertIn("not independently observed", report["security"]["authentication"])
            self.assertEqual(report["security"]["authorization"], "unknown")
        provider = aos.Node("provider", "/secure", context=producer_context)
        self.addCleanup(provider.__exit__, None, None, None)
        operator = aos.Node("operator", "/secure", context=consumer_context)
        self.addCleanup(operator.__exit__, None, None, None)
        subscription = operator.create_subscription(self.interface, "/secure/data")
        publisher = provider.create_publisher(self.interface, "/secure/data")
        deadline = time.monotonic() + 3
        while True:
            publisher.publish({"data": "encrypted path"})
            try:
                self.assertEqual(subscription.recv(0.1), {"data": "encrypted path"})
                break
            except aos.WaitTimeoutError:
                self.assertLess(time.monotonic(), deadline)
        resolved = self.cli("config", "resolve", {
            "security_profile": str(profiles["operator"]), "client_node": self.operator,
        }, environment=self.environment | {"AOS_SECURITY_PROFILE": "/missing/environment-profile"})
        self.assertTrue(resolved["data"]["security_profile_selected"])
        self.assertEqual(resolved["data"]["context"]["domain_id"]["value"], 29)
        self.assertFalse(resolved["data"]["network_opened"])
        live = self.cli("topic", "list", {
            "security_profile": str(profiles["operator"]), "timeout": 2,
        })
        self.assertIn("/secure/data", [item["name"] for item in live["data"]["topics"]])
        operator.destroy_node()
        provider.destroy_node()
        consumer_context.shutdown()
        producer_context.shutdown()
        started = time.monotonic()
        interrupt_process(router, signal.SIGTERM)
        stdout, stderr = router.communicate(timeout=4)
        self.assertEqual(router.returncode, 130 if os.name == "nt" else 143, stdout + stderr)
        self.assertLess(time.monotonic() - started, 3)
        summary = next(json.loads(line)["data"] for line in stdout.splitlines()
                       if json.loads(line)["event"] == "summary")
        self.assertTrue(summary["session_closed"], summary)
        self.assertIsNone(summary["cleanup_error"])

    def test_missing_cli_identity_is_rejected_before_connecting(self):
        profile = self.case / "unreachable.json"
        profile.write_text(json.dumps(self.client("operator", "tls/127.0.0.1:1")))
        topic = {"topic_name": "/secure/data"}
        action = {"action_name": "/secure/job", "action_type": "demo/action/Job"}
        node = {"node_name": "/secure/provider"}
        cases = [
            ("topic", "pub", topic | {"message_type": "std_msgs/msg/String",
                                     "values": {"data": "never sent"}}),
            *[("topic", verb, topic) for verb in ("echo", "hz", "bw", "delay")],
            ("service", "call", {"service_name": "/secure/service",
                                 "service_type": "demo/srv/Call", "values": {}}),
            ("action", "send_goal", action | {"goal": {}}),
            *[("action", verb, action | {"goal_id": "0" * 32})
              for verb in ("get_result", "cancel")],
            *[("param", verb, node | {"names": ["rate"]}) for verb in ("get", "describe")],
            ("param", "list", node), ("param", "dump", node),
            ("param", "set", node | {"parameters": {"rate": 2}}),
            ("param", "load", node | {"path": "/must-not-read-parameters.json"}),
            ("lifecycle", "get", node), ("lifecycle", "list", node),
            ("lifecycle", "set", node | {"transition": "activate"}),
        ]
        for group, verb, request in cases:
            with self.subTest(command=f"{group} {verb}"):
                response = self.cli(group, verb, request | {"security_profile": str(profile)}, code=2)
                error = response.get("error", response.get("data"))
                self.assertEqual(error["phase"], "configuration", response)
                self.assertIn("client_node", error["message"])
        # A Graph-only query has no local Node. It reaches session_open and
        # fails the actual connection, rather than the client identity check.
        response = self.cli("node", "list", {"security_profile": str(profile)}, code=3)
        self.assertEqual(response["error"]["phase"], "session_open", response)
        self.assertNotIn("client_node", response["error"]["message"])

    def test_schema_errors_do_not_echo_supplied_credential_material(self):
        marker = "-----BEGIN PRIVATE KEY-----synthetic-test-marker"
        response = self.cli("security", "client-config", {"client": marker}, code=2)
        self.assertNotIn(marker, json.dumps(response))
        response = self.cli("security", "client-config", {"client": {}, marker: "bad"}, code=2)
        self.assertNotIn(marker, json.dumps(response))
        result = subprocess.run(
            [str(self.binary), "security", "serve", "--request", "-"],
            input=json.dumps({"policy": {}, "router": marker}),
            capture_output=True, text=True, env=self.environment, **console_process_options(), timeout=5,
        )
        self.assertEqual(result.returncode, 2)
        self.assertNotIn(marker, result.stdout + result.stderr)
        report = aos.doctor_report(security_profile=marker, timeout=3)
        self.assertEqual(report["status"], "error", report)
        self.assertTrue(report["security"]["profile_selected"])
        self.assertIsNone(report["security"]["profile_path"])
        self.assertNotIn(marker, json.dumps(report))

    def test_secure_launch_checks_all_known_child_layers_before_spawning(self):
        marker = self.case / "spawned"
        prefix = self.install_entry(
            f"from pathlib import Path\nPath({str(marker)!r}).write_text('spawned')\n",
        )
        profiles = {}
        for name in ("provider", "operator"):
            path = self.case / f"{name}.json"
            path.write_text(json.dumps(self.client(name, "tls/127.0.0.1:1")))
            profiles[name] = str(path)
        other = self.case / "other-router.json"
        other.write_text(json.dumps(self.client("provider", "tls/127.0.0.1:2")))
        plaintext = self.case / "peer.json5"
        plaintext.write_text('{"mode":"peer","listen":{"endpoints":[]},'
                             '"scouting":{"multicast":{"enabled":false},"gossip":{"enabled":false}}}')
        processes = [
            {"id": name, "package": "secure_demo", "entry": "provider",
             "env": {"AOS_SECURITY_PROFILE": profiles["provider"]}}
            for name in ("first", "second")
        ]
        base = {
            "prefixes": [str(prefix)], "security_profile": profiles["operator"],
            "plan": {"schema_version": 1, "processes": processes},
        }
        cases = []
        missing = copy.deepcopy(base)
        missing["plan"]["processes"][1]["env"] = {}
        cases.append(("missing_profile", missing, {}))
        for field, value in (
            ("AOS_DOMAIN_ID", "30"),
            ("AOS_ZENOH_CONFIG", str(plaintext)),
            ("AOS_SECURITY_PROFILE", str(other)),
        ):
            request = copy.deepcopy(base)
            request["plan"]["processes"][1]["env"][field] = value
            cases.append((field, request, {}))
        cases.append(("inherited_domain", copy.deepcopy(base), {"AOS_DOMAIN_ID": "30"}))
        for source in ("launch", "cli"):
            request = copy.deepcopy(base)
            if source == "launch":
                request["plan"]["context"] = {"domain_id": 30, "zenoh_config": None}
            else:
                request["overrides"] = {"context": {"domain_id": 30}}
            cases.append((source, request, {}))
        for name, request, environment in cases:
            with self.subTest(case=name):
                result = subprocess.run(
                    [str(self.binary), "launch", "--request", "-"],
                    input=json.dumps(request), capture_output=True, text=True,
                    env=self.environment | environment, timeout=5,
                )
                self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
                events = [json.loads(line) for line in result.stdout.splitlines()]
                self.assertNotIn("process_started", [event["event"] for event in events])
                error = next(event["data"] for event in events if event["event"] == "error")
                self.assertEqual(error["phase"], "launch_preflight", error)
                self.assertFalse(marker.exists())
        # No readiness means no supervisor connection or client_node is needed.
        # Empty child domain values suppress the inherited environment value;
        # both children then use their profile default.
        valid = copy.deepcopy(base)
        for process in valid["plan"]["processes"]:
            process["env"]["AOS_DOMAIN_ID"] = ""
        result = subprocess.run(
            [str(self.binary), "launch", "--request", "-"],
            input=json.dumps(valid), capture_output=True, text=True,
            env=self.environment | {"AOS_DOMAIN_ID": "30"}, timeout=5,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue(marker.exists())
        self.assertNotIn('"event":"ready"', result.stdout)

    def test_secure_launch_readiness_uses_separate_control_and_child_profiles(self):
        _, profiles = self.start_router()
        prefix = self.install_entry(
            "import aos_sdk as aos, json, time\n"
            "with aos.Context() as context:\n"
            "    with aos.Node('declared', context=context, start_parameter_services=False):\n"
            "        print(json.dumps({'provider_started': True}), flush=True)\n"
            "        while True: time.sleep(0.1)\n",
        )
        request = {
            "prefixes": [str(prefix)], "security_profile": str(profiles["operator"]),
            "client_node": self.operator,
            "plan": {
                "schema_version": 1,
                "context": {"domain_id": 29, "zenoh_config": None},
                "processes": [{
                    "id": "provider", "package": "secure_demo", "entry": "provider",
                    "env": {"AOS_SECURITY_PROFILE": str(profiles["provider"])},
                    "node": self.provider,
                    "readiness": {"node": "/secure/provider", "timeout": 3},
                }],
            },
        }
        process = subprocess.Popen(
            [str(self.binary), "launch", "--request", "-"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, env=self.environment, **console_process_options(),
        )
        process.stdin.write(json.dumps(request))
        process.stdin.close()
        events = queue.Queue()
        trace = []

        def read():
            for line in process.stdout:
                value = json.loads(line)
                trace.append(value)
                events.put(value)

        reader = threading.Thread(target=read, daemon=True)
        reader.start()

        def cleanup():
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
            reader.join(timeout=1)
            process.stdout.close()
            process.stderr.close()

        self.addCleanup(cleanup)
        deadline = time.monotonic() + 6
        while True:
            event = events.get(timeout=max(0.01, deadline - time.monotonic()))
            if event["event"] == "ready":
                self.assertEqual(event["process_id"], "provider")
                break
            self.assertNotEqual(event["event"], "launch_failed", event)
            self.assertLess(time.monotonic(), deadline, trace)
        interrupt_process(process, signal.SIGTERM)
        self.assertEqual(process.wait(timeout=5), 130 if os.name == "nt" else 143)
        reader.join(timeout=1)
        self.assertFalse(reader.is_alive())
        summary = next(event["data"] for event in trace if event["event"] == "summary")
        self.assertEqual(summary["cleanup_errors"], [])
        self.assertEqual(summary["outcome"], "stopped")


if __name__ == "__main__":
    unittest.main()
