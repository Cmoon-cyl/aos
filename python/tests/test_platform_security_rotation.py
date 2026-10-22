"""Deployment revocation through CA replacement and a stopped/restarted router.

The installed CLI owns the real TLS listener. This test deliberately keeps an
old SDK Context alive across the restart. It tests withdrawal of an entire CA,
not leaf-serial revocation, CRL/OCSP, or certificate hot reload.
"""

import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
import unittest
from unittest import mock

import aos_sdk as aos
from process_support import console_process_options, interrupt_process, read_line


ROOT = Path(__file__).resolve().parents[2]


class SecurityRotationTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="aos-security-rotation-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.directory.chmod(0o700)
        self.binary = Path(os.environ["AOS_CLI"]).resolve()
        self.assertTrue(self.binary.is_file())
        self.assertFalse(Path(aos.__file__).resolve().is_relative_to(ROOT / "python"))
        self.environment = {key: value for key, value in os.environ.items()
                            if not key.startswith("AOS_")}
        environment = mock.patch.dict(os.environ, self.environment, clear=True)
        environment.start()
        self.addCleanup(environment.stop)
        self.routers = []
        self.addCleanup(self.cleanup_routers)
        self.interface = aos.Interface.load(ROOT / "interfaces/String.json")
        publisher = {"name": "publisher", "namespace": "/rotation"}
        observer = {"name": "observer", "namespace": "/rotation"}
        self.policy = {"schema_version": 1, "domain_id": 29, "subjects": [
            {"id": "publisher", "common_name": "publisher", "nodes": [publisher],
             "endpoints": [{
                 "node": publisher, "namespace": "/rotation", "name": "/rotation/data",
                 "interface": self.interface.definition, "operations": ["publish"],
             }]},
            {"id": "observer", "common_name": "observer", "nodes": [observer],
             "endpoints": [{
                 "node": observer, "namespace": "/rotation", "name": "/rotation/data",
                 "interface": self.interface.definition, "operations": ["subscribe"],
             }]},
        ]}
        for root in ("a", "b"):
            self.openssl(
                "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1",
                "-nodes", "-sha256", "-days", "2", "-subj", f"/CN=AOS rotation CA {root}",
                "-keyout", f"ca-{root}.key", "-out", f"ca-{root}.pem",
                "-addext", "basicConstraints=critical,CA:TRUE",
                "-addext", "keyUsage=critical,keyCertSign,cRLSign",
            )
        for serial, (name, cn, root) in enumerate((
            ("server-a", "router", "a"), ("server-b", "router", "b"),
            ("publisher-a", "publisher", "a"),
            ("publisher-a-reissued", "publisher", "a"),
            ("publisher-b", "publisher", "b"),
            ("observer-a", "observer", "a"), ("observer-b", "observer", "b"),
        ), start=1):
            self.issue(name, cn, root, serial)
        # The stale client can verify either server epoch. Its rejection after
        # restart must therefore not be explained by an unknown server CA.
        (self.directory / "ca-ab.pem").write_bytes(
            (self.directory / "ca-a.pem").read_bytes()
            + (self.directory / "ca-b.pem").read_bytes()
        )
        with socket.socket() as reserved:
            reserved.bind(("127.0.0.1", 0))
            self.port = reserved.getsockname()[1]
        self.endpoint = f"tls/127.0.0.1:{self.port}"

    def openssl(self, *args):
        result = subprocess.run(
            ["openssl", *args], cwd=self.directory, capture_output=True, text=True,
            timeout=15,
        )
        self.assertEqual(result.returncode, 0, "OpenSSL fixture operation failed")
        return result.stdout

    def issue(self, name, cn, root, serial):
        self.openssl(
            "req", "-new", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1",
            "-nodes", "-subj", f"/CN={cn}", "-keyout", f"{name}.key", "-out", f"{name}.csr",
        )
        extension = self.directory / f"{name}.ext"
        extension.write_text(
            "basicConstraints=critical,CA:FALSE\n"
            "keyUsage=critical,digitalSignature,keyAgreement\n"
            "extendedKeyUsage=serverAuth,clientAuth\n"
            "subjectAltName=IP:127.0.0.1,DNS:localhost\n",
        )
        self.openssl(
            "x509", "-req", "-sha256", "-days", "2", "-in", f"{name}.csr",
            "-CA", f"ca-{root}.pem", "-CAkey", f"ca-{root}.key", "-set_serial", str(serial),
            "-extfile", extension.name, "-out", f"{name}.pem",
        )

    def tls_files(self, certificate, root):
        return {
            "schema_version": 1, "root_ca_certificate": str(self.directory / f"ca-{root}.pem"),
            "certificate": str(self.directory / f"{certificate}.pem"),
            "private_key": str(self.directory / f"{certificate}.key"),
        }

    def profile(self, label, certificate, root):
        path = self.directory / f"{label}-profile.json"
        path.write_text(json.dumps(self.tls_files(certificate, root) | {
            "domain_id": 29, "router_endpoints": [self.endpoint], "connect_timeout_ms": 700,
        }))
        return path

    def node(self, profile, name):
        context = aos.Context(security_profile=profile)
        self.addCleanup(context.__exit__, None, None, None)
        node = aos.Node(name, "/rotation", context=context)
        self.addCleanup(node.__exit__, None, None, None)
        return context, node

    def start_router(self, epoch):
        request = {
            "router": self.tls_files(f"server-{epoch}", epoch)
            | {"listen_endpoints": [self.endpoint]},
            "policy": self.policy, "startup_timeout": 5, "shutdown_timeout": 1,
        }
        path = self.directory / f"router-{epoch}.json"
        path.write_text(json.dumps(request))
        stderr = (self.directory / f"router-{epoch}.stderr").open("w+")
        process = subprocess.Popen(
            [str(self.binary), "security", "serve", "--request", str(path)],
            env=self.environment, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=stderr, text=True, encoding="utf-8",
            **console_process_options(),
        )
        handle = {"process": process, "stderr": stderr, "stopped": False, "request": request}
        self.routers.append(handle)
        line = read_line(process.stdout, 6)
        self.assertTrue(line, "router exited before readiness")
        ready = json.loads(line)
        self.assertEqual(ready["event"], "router_ready", ready)
        self.assertEqual(ready["data"]["locators"], [self.endpoint])
        self.assertEqual(ready["data"]["domain_id"], 29)
        self.assertEqual(ready["data"]["policy_digest"],
                         aos.compile_security_policy(self.policy)["source_digest"])
        handle["ready"] = ready["data"]
        return handle

    def stop_router(self, handle):
        process = handle["process"]
        interrupt_process(process, signal.SIGTERM)
        stdout, _ = process.communicate(timeout=4)
        handle["stopped"] = True
        self.assertEqual(process.returncode, 130 if os.name == "nt" else 143, stdout)
        summaries = [json.loads(line)["data"] for line in stdout.splitlines()
                     if json.loads(line)["event"] == "summary"]
        self.assertEqual(len(summaries), 1, stdout)
        self.assertTrue(summaries[0]["session_closed"], summaries[0])
        self.assertIsNone(summaries[0]["cleanup_error"], summaries[0])
        handle["summary"] = summaries[0]

    def cleanup_routers(self):
        for handle in reversed(self.routers):
            process = handle["process"]
            if not handle["stopped"]:
                if process.poll() is None:
                    interrupt_process(process, signal.SIGTERM)
                try:
                    process.communicate(timeout=4)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.communicate(timeout=3)
            process.stdout.close()
            handle["stderr"].close()

    def deliver(self, publisher, subscription, marker):
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            publisher.publish({"data": marker})
            try:
                if subscription.recv(0.1) == {"data": marker}:
                    return
            except aos.WaitTimeoutError:
                pass
        self.fail("authorized Topic delivery did not converge")

    def test_ca_replacement_rejects_old_leaf_and_stale_connection_after_router_restart(self):
        router_a = self.start_router("a")
        old_profile = self.profile("old", "publisher-a", "ab")
        old_context, old_node = self.node(old_profile, "publisher")
        old_publisher = old_node.create_publisher(self.interface, "/rotation/data")
        observer_a_context, observer_a = self.node(
            self.profile("observer-a", "observer-a", "a"), "observer",
        )
        subscription_a = observer_a.create_subscription(self.interface, "/rotation/data")
        self.deliver(old_publisher, subscription_a, "before-rotation")

        # A new key/serial with the same issuer and CN is the same ACL subject.
        reissued_context, reissued_node = self.node(
            self.profile("reissued", "publisher-a-reissued", "a"), "publisher",
        )
        reissued = reissued_node.create_publisher(self.interface, "/rotation/data")
        self.deliver(reissued, subscription_a, "same-ca-same-cn-still-authorized")
        reissued_node.destroy_node()
        reissued_context.shutdown()
        observer_a.destroy_node()
        observer_a_context.shutdown()

        # Do not close old_context/old_node/old_publisher. Stopping the router
        # must end the old TLS link, and the new trust must reject reconnection.
        self.stop_router(router_a)
        router_b = self.start_router("b")
        self.assertNotEqual(router_a["ready"]["zid"], router_b["ready"]["zid"])
        new_context, new_node = self.node(
            self.profile("new", "publisher-b", "b"), "publisher",
        )
        new_publisher = new_node.create_publisher(self.interface, "/rotation/data")
        observer_b_context, observer_b = self.node(
            self.profile("observer-b", "observer-b", "b"), "observer",
        )
        subscription_b = observer_b.create_subscription(self.interface, "/rotation/data")
        self.deliver(new_publisher, subscription_b, "after-rotation")

        # This fresh client trusts server B but still presents the old A leaf.
        # A rejection therefore cannot be credited merely to server distrust.
        rejected_profile = self.profile("old-leaf-new-server-trust", "publisher-a", "b")
        started = time.monotonic()
        with self.assertRaises(aos.CommunicationError):
            unexpected = aos.Context(security_profile=rejected_profile)
            self.addCleanup(unexpected.__exit__, None, None, None)
        self.assertLess(time.monotonic() - started, 5)

        old_errors = []
        new_deliveries = 0
        window = 3.0
        deadline = time.monotonic() + window
        while time.monotonic() < deadline:
            try:
                old_publisher.publish({"data": "revoked-old-connection"})
            except (aos.CommunicationError, aos.InvalidStateError) as error:
                old_errors.append(type(error).__name__)
            new_publisher.publish({"data": "permitted-new-connection"})
            try:
                value = subscription_b.recv(0.1)
            except aos.WaitTimeoutError:
                continue
            self.assertNotEqual(value, {"data": "revoked-old-connection"})
            new_deliveries += value == {"data": "permitted-new-connection"}
            time.sleep(0.02)
        self.assertGreater(new_deliveries, 0)
        # Drain the bounded queue so a later old sample cannot hide behind the
        # positive control sample consumed on the final iteration.
        drain_deadline = time.monotonic() + 1
        while time.monotonic() < drain_deadline:
            try:
                value = subscription_b.recv(0.1)
            except aos.WaitTimeoutError:
                break
            self.assertNotEqual(value, {"data": "revoked-old-connection"})
        else:
            self.fail("Topic queue did not quiesce after the observation window")

        old_node.__exit__(None, None, None)
        old_context.__exit__(None, None, None)
        new_node.destroy_node()
        new_context.shutdown()
        observer_b.destroy_node()
        observer_b_context.shutdown()
        self.stop_router(router_b)
        public_fingerprints = {
            name: hashlib.sha256((self.directory / f"{name}.pem").read_bytes()).hexdigest()
            for name in ("ca-a", "ca-b", "server-a", "server-b", "publisher-a",
                         "publisher-a-reissued", "publisher-b")
        }
        self.assertEqual(len(set(public_fingerprints.values())), len(public_fingerprints))
        public_metadata = {
            name: self.openssl(
                "x509", "-in", f"{name}.pem", "-noout", "-subject", "-issuer", "-serial",
                "-dates", "-fingerprint", "-sha256",
            ).splitlines()
            for name in public_fingerprints
        }
        print(json.dumps({
            "case": "ca_replacement_router_restart", "status": "passed",
            "scope": "CA withdrawal; no per-leaf revocation or hot reload",
            "same_endpoint": self.endpoint, "same_cn": "publisher",
            "same_ca_reissue_still_authorized": True,
            "router_epochs": [
                {"pid": item["process"].pid, "zid": item["ready"]["zid"],
                 "policy_digest": item["ready"]["policy_digest"],
                 "session_closed": item["summary"]["session_closed"]}
                for item in (router_a, router_b)
            ],
            "old_leaf_with_new_server_trust": "connection_rejected",
            "old_connection_observation_seconds": window,
            "old_connection_publish_errors": sorted(set(old_errors)),
            "old_connection_received_samples": 0, "new_connection_samples": new_deliveries,
            "public_certificate_pem_sha256": public_fingerprints,
            "public_certificate_metadata": public_metadata,
            "deployment_requests": [router_a["request"], router_b["request"]],
            "client_profiles": {
                path.name: json.loads(path.read_text())
                for path in sorted(self.directory.glob("*-profile.json"))
            },
        }, sort_keys=True))
