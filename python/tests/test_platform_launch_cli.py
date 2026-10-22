"""Installed-prefix and real OS process acceptance for package/run/launch.

The ordinary fixture entries below intentionally do not import an SDK. They
prove package/process behavior only. SDK bootstrap/readiness needs the separate
installed-wheel cases; ProcessStarted is never their success criterion.
"""

import json
import os
from pathlib import Path
import queue
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from process_support import assert_canonical_path, console_process_options, interrupt_process, process_alive


class LaunchCliTest(unittest.TestCase):
    def setUp(self):
        self.binary = Path(os.environ["AOS_CLI"]).resolve()
        self.assertTrue(self.binary.is_file())
        temporary = tempfile.TemporaryDirectory(prefix="aos-process-cli-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name).resolve()
        self.prefix = self.directory / "installed"
        self.cwd = self.directory / "unrelated"
        self.cwd.mkdir()
        self.environment = {
            key: value for key, value in os.environ.items()
            if not key.startswith("AOS_")
        }
        self.manifest = self.install(self.prefix)

    def install(self, prefix, *, version="1.0.0"):
        (prefix / "share/demo/launch").mkdir(parents=True)
        (prefix / "lib/demo").mkdir(parents=True)
        index = prefix / "share/aos/resource_index/packages"
        index.mkdir(parents=True)
        manifest = {
            "schema_version": 1, "name": "demo", "version": version,
            "runtime_abi": "aos.process.v1",
            "entries": {}, "resources": {},
        }
        (index / "demo.json").write_text(json.dumps(manifest))
        return manifest

    def save(self):
        path = self.prefix / "share/aos/resource_index/packages/demo.json"
        path.write_text(json.dumps(self.manifest))

    def entry(self, name, source):
        path = self.prefix / "lib/demo" / name
        path.write_text("#!" + sys.executable + "\n" + source)
        path.chmod(0o755)
        self.manifest["entries"][name] = {
            "executable": path.relative_to(self.prefix).as_posix(), "kind": "aos_node",
            **({"interpreter": sys.executable, "interpreter_args": ["-u"]} if os.name == "nt" else {}),
        }
        self.save()
        return path

    def start(self, group, request, verb="start", *, env=None, binary=None,
              stdout=subprocess.PIPE, detached=False):
        args = [str(binary or self.binary), group]
        if verb:
            args.append(verb)
        process = subprocess.Popen(
            args + ["--request", "-"], cwd=self.cwd,
            stdin=subprocess.PIPE, stdout=stdout, stderr=subprocess.PIPE,
            text=True, env=self.environment | (env or {}),
            **({"creationflags": 0x8} if detached and os.name == "nt" else console_process_options()),
        )
        process.stdin.write(json.dumps(request))
        process.stdin.close()
        process.stdin = None

        def stop():
            if process.poll() is None:
                process.terminate()
                try:
                    process.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.communicate(timeout=5)

        self.addCleanup(stop)
        return process

    def finish(self, process, expected=0, timeout=10):
        output, error = process.communicate(timeout=timeout)
        self.assertEqual(process.returncode, expected, output + error)
        return [json.loads(line) for line in output.splitlines()], error

    def run_request(self, entry, **options):
        return {"prefixes": [str(self.prefix)], "package": "demo", "entry": entry, **options}

    def test_installed_lookup_from_arbitrary_cwd_and_explicit_prefix_isolation(self):
        self.entry("hello", "print('hello')\n")
        resource = self.prefix / "share/demo/launch/empty.json"
        resource.write_text("{}")
        self.manifest["resources"]["plan"] = {
            "kind": "launch", "path": resource.relative_to(self.prefix).as_posix(),
        }
        self.save()
        env = {"AOS_PREFIX_PATH": str(self.directory / "missing"),
               "AOS_ZENOH_CONFIG": str(self.directory / "missing-session")}
        for verb, request in [
            ("list", {}),
            ("info", {"package": "demo"}),
            ("resource", {"package": "demo", "resource": "plan"}),
        ]:
            output, _ = self.finish(self.start("package",
                {"prefixes": [str(self.prefix)], **request}, verb, env=env))
            self.assertTrue(output[0]["ok"])
            if verb == "resource":
                assert_canonical_path(output[0]["data"]["path"], resource)
        output, _ = self.finish(self.start("package", {}, "list"))
        self.assertEqual(output[0]["data"]["packages"], {})
        output, _ = self.finish(self.start("package", {"package": "demo"}, "info",
            env={"AOS_PREFIX_PATH": str(self.prefix)}))
        self.assertEqual(output[0]["data"]["manifest"]["version"], "1.0.0")

    def test_installed_binary_discovers_its_own_prefix(self):
        self.entry("hello", "print('hello')\n")
        executable = self.prefix / "bin/aos"
        executable.parent.mkdir()
        shutil.copy2(self.binary, executable)
        output, _ = self.finish(self.start("package", {"package": "demo"}, "info", binary=executable))
        assert_canonical_path(output[0]["data"]["prefix"], self.prefix)
        output, _ = self.finish(self.start("package", {"package": "demo"}, "info",
            binary=executable, env={"AOS_PREFIX_PATH": ""}))
        assert_canonical_path(output[0]["data"]["prefix"], self.prefix)
        output, _ = self.finish(self.start("package", {}, "list", binary=executable,
            env={"AOS_PROCESS_CONFIG": "/unused/bootstrap", "AOS_PREFIX_PATH": ""}))
        self.assertEqual(output[0]["data"]["packages"], {})
        output, _ = self.finish(self.start("package", {}, "list",
            env={"AOS_PREFIX_PATH": str(self.prefix) + os.pathsep}), 2)
        self.assertEqual(output[0]["error"]["code"], "INVALID_ARGUMENT")

    def test_missing_ambiguous_and_version_mismatch_are_preflight_errors(self):
        self.entry("hello", "print('hello')\n")
        other = self.directory / "other"
        self.install(other, version="2.0.0")
        cases = [
            ({"package": "absent", "prefixes": [str(self.prefix)]}, "PACKAGE_NOT_FOUND", 3),
            ({"package": "demo", "prefixes": [str(self.prefix), str(other)]}, "AMBIGUOUS_PACKAGE", 3),
            ({"package": "demo", "package_version": "2.0.0",
              "prefixes": [str(self.prefix)]}, "VERSION_MISMATCH", 2),
        ]
        for request, code, status in cases:
            with self.subTest(code=code):
                output, _ = self.finish(self.start("package", request, "info"), status)
                self.assertEqual(output[0]["error"]["code"], code)

    def test_run_without_readiness_never_opens_network_or_claims_ready(self):
        self.entry("report", """
import json, os, pathlib, sys
print(json.dumps({"cwd": os.getcwd(), "args": sys.argv[1:]}), flush=True)
print("ordinary stdout", flush=True)
print("diagnostic stderr", file=sys.stderr, flush=True)
""")
        unopened_config = self.directory / "not-a-session.json5"
        unopened_config.write_text("This is not a Zenoh session configuration.")
        output, error = self.finish(self.start("run", self.run_request(
            "report", args=["one value", "$literal;not-a-shell"]), verb=None,
            env={"AOS_ZENOH_CONFIG": str(unopened_config)}))
        self.assertIn("process_started", [event["event"] for event in output])
        self.assertNotIn("ready", [event["event"] for event in output])
        business = next(event["details"]["value"] for event in output if event["event"] == "process_stdout")
        assert_canonical_path(business["cwd"], self.prefix / "share/demo")
        self.assertEqual(business["args"], ["one value", "$literal;not-a-shell"])
        self.assertIn("ordinary stdout", error)
        self.assertIn("diagnostic stderr", error)
        summary = next(event["data"] for event in output if event["event"] == "summary")
        self.assertEqual(summary["outcome"], "completed")
        self.assertFalse(summary["processes"][0]["ready"])

    def test_all_entries_are_checked_before_any_child_starts(self):
        marker = self.directory / "must-not-run"
        self.entry("first", f"from pathlib import Path\nPath({str(marker)!r}).write_text('started')\n")
        plan = {
            "schema_version": 1,
            "processes": [
                {"id": "first", "package": "demo", "entry": "first"},
                {"id": "bad", "package": "demo", "entry": "missing"},
            ],
        }
        output, _ = self.finish(self.start("launch",
            {"prefixes": [str(self.prefix)], "plan": plan}), 3)
        self.assertEqual(output[-1]["data"]["code"], "ENTRY_NOT_FOUND")
        self.assertNotIn("process_started", [event["event"] for event in output])
        self.assertFalse(marker.exists())

        if os.name == "nt":
            bad = self.entry("command-file", "print('must not run')\n")
            for interpreter in (None, str(bad), "python.exe"):
                entry = self.manifest["entries"]["command-file"]
                entry.pop("interpreter", None)
                entry.pop("interpreter_args", None)
                if interpreter is not None:
                    entry["interpreter"] = interpreter
                self.save()
                plan["processes"][1]["entry"] = "command-file"
                output, _ = self.finish(self.start("launch", {
                    "prefixes": [str(self.prefix)], "plan": plan}), 2)
                self.assertNotIn("process_started", [e["event"] for e in output])
                self.assertFalse(marker.exists())

    def test_file_and_resource_plan_paths_use_the_document_directory(self):
        self.entry("bootstrap", """
import json, os, pathlib
value = json.loads(pathlib.Path(os.environ["AOS_PROCESS_CONFIG"]).read_text())
print(json.dumps(value), flush=True)
""")
        document = self.prefix / "share/demo/launch/example.json"
        config = document.parent / "parameters.json"
        config.write_text('{"parameters":{"limit":3}}')
        document.write_text(json.dumps({
            "schema_version": 1,
            "processes": [{"id": "worker", "package": "demo", "entry": "bootstrap",
                           "config_files": ["parameters.json"]}],
        }))
        self.manifest["resources"]["example"] = {
            "kind": "launch", "path": str(document.relative_to(self.prefix)),
        }
        self.save()
        for source in ({"path": str(document)}, {"package": "demo", "resource": "example"}):
            output, _ = self.finish(self.start("launch",
                {"prefixes": [str(self.prefix)], **source}, verb=None))
            bootstrap = next(e["details"]["value"] for e in output if e["event"] == "process_stdout")
            self.assertEqual(len(bootstrap["node"]["config_files"]), 1)
            assert_canonical_path(bootstrap["node"]["config_files"][0], config)
            self.assertNotIn("ready", [e["event"] for e in output])

    def test_environment_paths_are_fixed_before_child_cwd_changes(self):
        self.entry("environment", """
import json, os
print(json.dumps({key:os.environ[key] for key in
    ["AOS_ZENOH_CONFIG","AOS_INTERFACE_PATH"]}), flush=True)
""")
        document = self.prefix / "share/demo/launch/environment.json"
        self.manifest["resources"]["environment"] = {
            "kind": "launch", "path": str(document.relative_to(self.prefix)),
        }
        self.save()
        for base in [document.parent, self.cwd]:
            (base / "session.json5").write_text("not opened without readiness")
            (base / "interfaces").mkdir()
        for explicit in [True, False]:
            process = {"id": "worker", "package": "demo", "entry": "environment"}
            environment = {"AOS_ZENOH_CONFIG": "session.json5",
                           "AOS_INTERFACE_PATH": "interfaces"}
            if explicit:
                process["env"] = environment
            document.write_text(json.dumps({"schema_version": 1, "processes": [process]}))
            for source in [{"path": str(document)},
                           {"package": "demo", "resource": "environment"}]:
                with self.subTest(explicit=explicit, source=tuple(source)):
                    output, _ = self.finish(self.start("launch",
                        {"prefixes": [str(self.prefix)], **source},
                        env={} if explicit else environment))
                    actual = next(e["details"]["value"] for e in output
                                  if e["event"] == "process_stdout")
                    base = document.parent if explicit else self.cwd
                    self.assertEqual(set(actual), {"AOS_ZENOH_CONFIG", "AOS_INTERFACE_PATH"})
                    assert_canonical_path(actual["AOS_ZENOH_CONFIG"], base / "session.json5")
                    assert_canonical_path(actual["AOS_INTERFACE_PATH"], base / "interfaces")

    def test_plan_rejects_duplicate_json_keys_and_conflicting_sources(self):
        document = self.directory / "duplicate.json"
        document.write_text('{"schema_version":1,"schema_version":1,"processes":[]}')
        for request in [
            {"path": str(document)},
            {"path": str(document), "plan": {"schema_version": 1, "processes": []}},
        ]:
            output, _ = self.finish(self.start("launch",
                {"prefixes": [str(self.prefix)], **request}), 2)
            self.assertEqual(output[-1]["event"], "error")
            self.assertEqual(output[-1]["data"]["code"], "INVALID_ARGUMENT")

    @unittest.skipUnless(os.name == "posix", "Unix process contract")
    def test_spawn_failure_cleans_an_already_started_peer(self):
        self.entry("persistent", "import time\ntime.sleep(30)\n")
        broken = self.entry("broken", "pass\n")
        # It is a valid installed executable path, but the OS cannot resolve
        # its interpreter. This reaches spawn failure after full preflight.
        broken.write_text("#!/aos-test-missing-interpreter\n")
        plan = {
            "schema_version": 1,
            "shutdown": {"sigint_timeout": .05, "sigterm_timeout": .05},
            "processes": [
                {"id": "first", "package": "demo", "entry": "persistent"},
                {"id": "broken", "package": "demo", "entry": "broken"},
            ],
        }
        output, _ = self.finish(self.start("launch",
            {"prefixes": [str(self.prefix)], "plan": plan}), 1)
        first = next(e for e in output if e["event"] == "process_started")
        self.assert_dead(first["pid"])
        summary = next(e["data"] for e in output if e["event"] == "summary")
        self.assertEqual(summary["outcome"], "failed")
        self.assertEqual(summary["cleanup_errors"], [])
        self.assertEqual(output[-1]["data"]["code"], "SPAWN_FAILED")

    def test_on_failure_restart_limit_and_no_restart_after_zero_exit(self):
        self.entry("failure", "raise SystemExit(7)\n")
        output, _ = self.finish(self.start("run", self.run_request("failure",
            restart={"policy": "on_failure", "delay": .03, "max_restarts": 2})), 1)
        self.assertEqual(sum(e["event"] == "process_started" for e in output), 3)
        self.assertEqual(sum(e["event"] == "restart_scheduled" for e in output), 2)
        summary = next(e["data"] for e in output if e["event"] == "summary")
        self.assertEqual(summary["processes"][0]["restarts"], 2)
        self.assertEqual(summary["outcome"], "failed")
        self.entry("success", "raise SystemExit(0)\n")
        output, _ = self.finish(self.start("run", self.run_request("success",
            restart={"policy": "on_failure", "delay": 0, "max_restarts": 2})))
        self.assertEqual(sum(e["event"] == "process_started" for e in output), 1)
        self.assertNotIn("restart_scheduled", [e["event"] for e in output])

    def test_same_named_external_node_cannot_release_a_dependency(self):
        import aos_sdk as aos
        from test_stage1_cli import RouterProcess

        router = RouterProcess()
        self.addCleanup(router.close)
        context = aos.Context(config=router.config, domain_id=79)
        self.addCleanup(context.shutdown)
        external = aos.Node("external", "/launch_acceptance", context)
        self.addCleanup(external.destroy_node)
        self.entry("persistent", "import time\ntime.sleep(30)\n")
        marker = self.directory / "consumer-started"
        self.entry("consumer", f"from pathlib import Path\nPath({str(marker)!r}).write_text('started')\n")
        plan = {
            "schema_version": 1,
            "context": {"domain_id": 79, "zenoh_config": str(router.config)},
            "shutdown": {"sigint_timeout": .05, "sigterm_timeout": .05},
            "processes": [
                {"id": "owned", "package": "demo", "entry": "persistent",
                 "readiness": {"node": external.fully_qualified_name, "timeout": .6}},
                {"id": "consumer", "package": "demo", "entry": "consumer", "depends_on": ["owned"]},
            ],
        }
        output, _ = self.finish(self.start("launch",
            {"prefixes": [str(self.prefix)], "plan": plan}), 4)
        self.assertEqual([e["process_id"] for e in output if e["event"] == "process_started"], ["owned"])
        self.assertNotIn("ready", [e["event"] for e in output])
        self.assertFalse(marker.exists())
        self.assertEqual(output[-1]["data"]["code"], "READINESS_TIMEOUT")

    def test_real_sdk_zero_delay_restart_requires_each_new_generation_ready(self):
        from test_stage1_cli import RouterProcess

        router = RouterProcess()
        self.addCleanup(router.close)
        restart_marker = self.directory / "restart-request"
        self.entry("sdk-restart", """
import aos_sdk as aos
import time, os, json
from pathlib import Path
context = aos.Context()
node = aos.Node("restart_worker", "/launch_r2", context)
while True:
    time.sleep(.02)
    marker = Path(os.environ["AOS_RESTART_MARKER"])
    generation = json.loads(Path(os.environ["AOS_PROCESS_CONFIG"]).read_text())["generation"]
    if marker.exists() and marker.read_text() == str(generation):
        raise SystemExit(7)
""")
        process = self.start("run", self.run_request("sdk-restart",
            env={"AOS_RESTART_MARKER": str(restart_marker)},
            context={"domain_id": 74017, "zenoh_config": str(router.config)},
            readiness={"node": "/launch_r2/restart_worker", "timeout": 5},
            restart={"policy": "on_failure", "delay": 0, "max_restarts": 2},
            shutdown={"sigint_timeout": .1, "sigterm_timeout": .1}))
        events = queue.Queue()
        output = []

        def read():
            for line in process.stdout:
                event = json.loads(line)
                output.append(event)
                events.put(event)

        reader = threading.Thread(target=read, daemon=True)
        reader.start()
        owned = []
        for generation in (1, 2, 3):
            deadline = time.monotonic() + 7
            while True:
                self.assertLess(time.monotonic(), deadline, output)
                try:
                    event = events.get(timeout=.1)
                except queue.Empty:
                    self.assertIsNone(process.poll(), output)
                    continue
                if event["event"] == "ready" and event["generation"] == generation:
                    break
            pid = event["pid"]
            owned.append(pid)
            if generation != 3:
                restart_marker.write_text(str(generation))
        interrupt_process(process, signal.SIGTERM)
        process.wait(timeout=5)
        reader.join(timeout=2)
        self.assertFalse(reader.is_alive())
        error = process.stderr.read()
        self.assertEqual(process.returncode, 130 if os.name == "nt" else 143, str(output) + error)
        self.assertEqual([e["generation"] for e in output if e["event"] == "ready"], [1, 2, 3])
        summary = next(e["data"] for e in output if e["event"] == "summary")
        self.assertEqual(summary["processes"][0]["restarts"], 2)
        self.assertEqual(summary["cleanup_errors"], [])
        for pid in owned:
            self.assert_dead(pid)
        process.stdout.close()
        process.stderr.close()

    def test_signals_interrupt_blocked_session_setup_before_any_child_or_run_directory(self):
        marker = self.directory / "startup-child"
        self.entry("startup", f"from pathlib import Path\nPath({str(marker)!r}).write_text('started')\n")
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            listener.settimeout(5)
            config = self.directory / "silent-peer.json5"
            config.write_text(json.dumps({
                "mode": "client",
                "connect": {"endpoints": [f"tcp/127.0.0.1:{listener.getsockname()[1]}"],
                            "timeout_ms": 10000, "exit_on_failure": True},
                "listen": {"endpoints": []},
                "scouting": {"multicast": {"enabled": False}, "gossip": {"enabled": False}},
            }))
            for sent in ((signal.SIGINT,) if os.name == "nt" else (signal.SIGINT, signal.SIGTERM)):
                with self.subTest(signal=sent):
                    temporary = self.directory / f"startup-tmp-{sent}"
                    temporary.mkdir()
                    process = self.start("run", self.run_request("startup",
                        context={"domain_id": 74018, "zenoh_config": str(config)},
                        readiness={"node": "/startup", "timeout": 5}),
                        env={"TMPDIR": str(temporary), "TMP": str(temporary), "TEMP": str(temporary)})
                    accepted, _ = listener.accept()
                    with accepted:
                        self.assertIsNone(process.poll())
                        self.assertFalse(marker.exists())
                        began = time.monotonic()
                        interrupt_process(process, sent)
                        output, error = process.communicate(timeout=1)
                        self.assertEqual(process.returncode, 0xC000013A if os.name == "nt" else -sent, output + error)
                        self.assertLess(time.monotonic() - began, 1)
                    self.assertFalse(marker.exists())
                    self.assertEqual(list(temporary.iterdir()), [])

    @unittest.skipUnless(os.name == "posix", "Unix process group contract")
    def test_all_terminal_signals_clean_the_owned_process_group(self):
        marker = self.directory / "identities.json"
        self.entry("persistent", """
import json, os, pathlib, signal, subprocess, sys, time
signal.signal(signal.SIGINT, signal.SIG_IGN)
signal.signal(signal.SIGTERM, signal.SIG_IGN)
child = subprocess.Popen([sys.executable, "-c",
    "import signal,time;signal.signal(signal.SIGINT,signal.SIG_IGN);signal.signal(signal.SIGTERM,signal.SIG_IGN);time.sleep(30)"])
pathlib.Path(sys.argv[1]).write_text(json.dumps({"leader":os.getpid(),"child":child.pid}))
print(json.dumps({"alive": True}), flush=True)
while True: time.sleep(.1)
""")
        for sent, status in [(signal.SIGHUP, 129), (signal.SIGINT, 130),
                             (signal.SIGQUIT, 131), (signal.SIGTERM, 143)]:
            with self.subTest(signal=sent):
                marker.unlink(missing_ok=True)
                process = self.start("run", self.run_request("persistent", args=[str(marker)],
                    shutdown={"sigint_timeout": .05, "sigterm_timeout": .05}))
                deadline = time.monotonic() + 4
                while not marker.exists() and time.monotonic() < deadline:
                    time.sleep(.01)
                self.assertTrue(marker.exists())
                identities = json.loads(marker.read_text())
                process.send_signal(sent)
                output, _ = self.finish(process, status)
                summary = next(e["data"] for e in output if e["event"] == "summary")
                self.assertEqual(summary["stop_reason"], {"kind": "signal", "detail": sent})
                self.assertEqual(summary["cleanup_errors"], [])
                for pid in identities.values():
                    self.assert_dead(pid)

    def assert_dead(self, pid):
        deadline = time.monotonic() + 2
        while process_alive(pid) and time.monotonic() < deadline:
            time.sleep(.02)
        self.assertFalse(process_alive(pid))

    def read_marker(self, path, parse=json.loads):
        deadline = time.monotonic() + 5
        while True:
            try:
                return parse(path.read_text())
            except (OSError, ValueError):
                self.assertLess(time.monotonic(), deadline, f"incomplete marker: {path}")
                time.sleep(.01)

    def test_closed_stdout_cleans_started_child(self):
        marker = self.directory / "output-owner"
        self.entry("output", """
import os, pathlib, sys, time
pathlib.Path(sys.argv[1]).write_text(str(os.getpid()))
while True:
    print('{"data": "continuous"}', flush=True)
    time.sleep(.005)
""")
        process = self.start("run", self.run_request("output", args=[str(marker)],
            shutdown={"sigint_timeout": .05, "sigterm_timeout": .05}))
        deadline = time.monotonic() + 4
        while not marker.exists() and time.monotonic() < deadline:
            time.sleep(.01)
        self.assertTrue(marker.exists())
        pid = int(marker.read_text())
        process.stdout.close()
        process.stdout = None
        _, error = process.communicate(timeout=5)
        self.assertNotEqual(process.returncode, 0, error)
        self.assert_dead(pid)

    def test_permanently_unconsumed_stdout_exits_after_owned_group_cleanup(self):
        marker = self.directory / "blocked-output-owner"
        self.entry("flood", """
import json, os, pathlib, signal, subprocess, sys, time
child = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(30)"])
pathlib.Path(sys.argv[1]).write_text(json.dumps({"leader":os.getpid(),"child":child.pid}))
time.sleep(.05)
stream = sys.stdout if sys.argv[2] == "stdout" else sys.stderr
while True:
    print(json.dumps({"data":"x"*8192}), file=stream, flush=True)
""")
        for stream in ("stdout",):
            with self.subTest(stream=stream):
                marker.unlink(missing_ok=True)
                process = self.start("run", self.run_request("flood", args=[str(marker), stream],
                    shutdown={"sigint_timeout": .05, "sigterm_timeout": .05}))
                deadline = time.monotonic() + 4
                while not marker.exists() and time.monotonic() < deadline:
                    time.sleep(.01)
                self.assertTrue(marker.exists())
                identities = json.loads(marker.read_text())
                began = time.monotonic()
                # Keep both read ends open and consume neither. This must
                # exercise WouldBlock, not BrokenPipe.
                process.wait(timeout=38)
                self.assertGreaterEqual(time.monotonic() - began, 29)
                self.assertLess(time.monotonic() - began, 38)
                output, error = process.communicate(timeout=2)
                self.assertEqual(process.returncode, 1, output + error)
                for pid in identities.values():
                    self.assert_dead(pid)

    @unittest.skipUnless(os.name == "posix", "Unix journal stream backpressure contract")
    def test_journal_like_stall_keeps_control_running_and_drains_after_children_are_gone(self):
        marker = self.directory / "journal-owner"
        self.entry("journal", """
import json, os, pathlib, signal, subprocess, sys, time
signal.signal(signal.SIGINT, signal.SIG_IGN)
signal.signal(signal.SIGTERM, signal.SIG_IGN)
child = subprocess.Popen([sys.executable, "-c",
    "import signal,time;signal.signal(signal.SIGINT,signal.SIG_IGN);signal.signal(signal.SIGTERM,signal.SIG_IGN);time.sleep(30)"])
pathlib.Path(sys.argv[1]).write_text(json.dumps({"leader":os.getpid(),"child":child.pid}))
sequence = 0
while True:
    print(json.dumps({"sequence":sequence,"payload":"x"*8192}), flush=True)
    sequence += 1
""")
        # journald uses a Unix stream. Pause this owned receiver instead of
        # stopping or reconfiguring the host's shared journal service.
        receiver, writer = socket.socketpair()
        self.addCleanup(receiver.close)
        self.addCleanup(writer.close)
        process = self.start("run", self.run_request("journal", args=[str(marker)],
            shutdown={"sigint_timeout": .05, "sigterm_timeout": .05}), stdout=writer)
        writer.close()
        deadline = time.monotonic() + 4
        while not marker.exists() and time.monotonic() < deadline:
            time.sleep(.01)
        self.assertTrue(marker.exists())
        owned = json.loads(marker.read_text())
        time.sleep(2.5)
        self.assertIsNone(process.poll(), "a short journal pause stopped the launch")
        process.terminate()
        for pid in owned.values():
            self.assert_dead(pid)
        self.assertIsNone(process.poll(), "control finished before blocked output was resumed")
        chunks = []

        def read():
            while chunk := receiver.recv(65536):
                chunks.append(chunk)

        reader = threading.Thread(target=read, daemon=True)
        reader.start()
        _, error = process.communicate(timeout=8)
        reader.join(timeout=2)
        self.assertFalse(reader.is_alive())
        self.assertEqual(process.returncode, 143, error)
        output = [json.loads(line) for line in b"".join(chunks).splitlines()]
        summary = next(e["data"] for e in output if e["event"] == "summary")
        self.assertEqual(summary["stop_reason"], {"kind": "signal", "detail": signal.SIGTERM})
        self.assertEqual(summary["cleanup_errors"], [])
        self.assertNotIn("CLEANUP_FAILED", str(summary))

    def test_block_buffered_short_json_lines_are_complete_and_ordered(self):
        self.entry("short-lines", """
import json, sys
sys.stdout.write("".join(json.dumps({"sequence":i}) + "\\n" for i in range(5000)))
""")
        output, _ = self.finish(self.start("run", self.run_request("short-lines")))
        values = [e["details"]["value"]["sequence"] for e in output
                  if e["event"] == "process_stdout"]
        self.assertEqual(values, list(range(5000)))
        self.assertNotIn("output_dropped", [e["event"] for e in output])

    def test_short_stdout_pause_recovers_complete_ordered_json(self):
        marker = self.directory / "burst-started"
        self.entry("burst", """
import json, pathlib, sys
pathlib.Path(sys.argv[1]).write_text("started")
for sequence in range(800):
    print(json.dumps({"sequence":sequence,"payload":"x"*8192}), flush=True)
""")
        process = self.start("run", self.run_request("burst", args=[str(marker)]))
        deadline = time.monotonic() + 4
        while not marker.exists() and time.monotonic() < deadline:
            time.sleep(.01)
        self.assertTrue(marker.exists())
        time.sleep(.2)
        output, _ = self.finish(process, timeout=10)
        values = [event["details"]["value"]["sequence"] for event in output
                  if event["event"] == "process_stdout"]
        self.assertEqual(values, list(range(800)))
        self.assertEqual(output[-1]["data"]["outcome"], "completed")

    def test_unconsumed_stderr_drops_logs_without_stopping_business(self):
        self.entry("diagnostics", """
import json, sys
for sequence in range(1200):
    print("diagnostic " + "x"*8192, file=sys.stderr, flush=True)
print(json.dumps({"business":"completed"}), flush=True)
""")
        process = self.start("run", self.run_request("diagnostics"))
        stdout = []
        reader = threading.Thread(target=lambda: stdout.append(process.stdout.read()), daemon=True)
        reader.start()
        process.wait(timeout=7)
        reader.join(timeout=1)
        self.assertFalse(reader.is_alive())
        self.assertEqual(process.returncode, 0)
        output = [json.loads(line) for line in stdout[0].splitlines()]
        self.assertTrue(any(e["event"] == "output_dropped" and e["details"]["lines"] > 0
                            for e in output))
        self.assertEqual(next(e["details"]["value"] for e in output
                              if e["event"] == "process_stdout"), {"business": "completed"})
        self.assertEqual(output[-1]["data"]["outcome"], "completed")
        complete_logs = 0
        for line in process.stderr.read().splitlines():
            try:
                json.loads(line)
                complete_logs += 1
            except json.JSONDecodeError:
                # A permanently blocked final diagnostic frame may be partial;
                # it must be included in the reported unfinished record count.
                pass
        dropped = sum(e["details"]["lines"] for e in output if e["event"] == "output_dropped")
        self.assertEqual(complete_logs + dropped, 1200)
        process.stdout.close()
        process.stderr.close()

    @unittest.skipUnless(os.name == "posix", "Unix shared descriptor contract")
    def test_terminal_errors_are_bounded_and_preserve_shared_descriptor_flags(self):
        import fcntl

        marker = self.directory / "terminal-error-child"
        self.entry("failure", f"import os,pathlib\npathlib.Path({str(marker)!r}).write_text(str(os.getpid()))\nraise SystemExit(7)\n")
        cases = [
            ({"package": "demo", "unexpected": True}, 2),
            (self.run_request("failure"), 1),
        ]
        for request, expected in cases:
            with self.subTest(expected=expected):
                read_fd, write_fd = os.pipe()
                process = None
                try:
                    original = fcntl.fcntl(write_fd, fcntl.F_GETFL)
                    os.set_blocking(write_fd, False)
                    while True:
                        try:
                            os.write(write_fd, b"x" * 4096)
                        except BlockingIOError:
                            break
                    fcntl.fcntl(write_fd, fcntl.F_SETFL, original)
                    # macOS may add an internal flag during F_SETFL. Compare
                    # against the actual restored descriptor, before spawn.
                    original = fcntl.fcntl(write_fd, fcntl.F_GETFL)
                    self.assertFalse(original & os.O_NONBLOCK)
                    process = subprocess.Popen(
                        [str(self.binary), "run", "--request", "-"], cwd=self.cwd,
                        stdin=subprocess.PIPE, stdout=write_fd, stderr=subprocess.DEVNULL,
                        env=self.environment,
                    )
                    process.stdin.write(json.dumps(request).encode())
                    process.stdin.close()
                    began = time.monotonic()
                    while process.poll() is None and time.monotonic() - began < 3.5:
                        self.assertEqual(fcntl.fcntl(write_fd, fcntl.F_GETFL), original)
                        time.sleep(.02)
                    if process.poll() is None:
                        process.kill()
                        process.wait(timeout=2)
                        self.fail("terminal output blocked CLI beyond its shared deadline")
                    self.assertEqual(process.returncode, expected)
                    self.assertEqual(fcntl.fcntl(write_fd, fcntl.F_GETFL), original)
                    if marker.exists():
                        self.assert_dead(int(marker.read_text()))
                finally:
                    os.close(read_fd)
                    os.close(write_fd)
                    if process is not None:
                        try:
                            process.wait(timeout=3)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait(timeout=2)

    def test_raw_json_numbers_are_preserved_and_duplicate_keys_are_diagnostics(self):
        raw = '{"id":12345678901234567890123,"decimal":1.0000000000000000001,"huge":1e400}'
        self.entry("numbers", f"print({raw!r}, flush=True)\nprint('{{\"same\":1,\"same\":2}}', flush=True)\n")
        process = self.start("run", self.run_request("numbers"))
        stdout, stderr = process.communicate(timeout=5)
        self.assertEqual(process.returncode, 0, stdout + stderr)
        self.assertIn(raw, stdout)
        output = [json.loads(line) for line in stdout.splitlines()]
        self.assertEqual(sum(e["event"] == "process_stdout" for e in output), 1)
        self.assertIn(r'\"same\":1,\"same\":2', stderr)

    def test_inline_file_and_resource_plans_apply_the_same_schema(self):
        self.entry("hello", "print('hello')\n")
        document = self.prefix / "share/demo/launch/schema.json"
        self.manifest["resources"]["schema"] = {
            "kind": "launch", "path": str(document.relative_to(self.prefix)),
        }
        self.save()
        base = {"schema_version": 1,
                "processes": [{"id": "worker", "package": "demo", "entry": "hello"}]}
        bad = []
        for change in [
            {"context": {"domain_id": 0}},
            {"shutdown": {"sigint_timeout": 86401}},
        ]:
            bad.append(base | change)
        for change in [
            {"restart": {"max_restarts": 4294967295}},
            {"readiness": {"node": "/worker", "lifecycle": {
                "target": "inactive", "autostart": True}}},
        ]:
            bad.append(base | {"processes": [base["processes"][0] | change]})
        for plan in bad:
            document.write_text(json.dumps(plan))
            for source in [{"plan": plan}, {"path": str(document)},
                           {"package": "demo", "resource": "schema"}]:
                with self.subTest(source=tuple(source), plan=plan):
                    output, _ = self.finish(self.start("launch",
                        {"prefixes": [str(self.prefix)], **source}), 2)
                    self.assertEqual(output[-1]["data"]["code"], "INVALID_ARGUMENT")
                    self.assertNotIn("process_started", [e["event"] for e in output])

    def test_detached_console_allocation_preserves_redirected_output(self):
        self.entry("detached", "import json\nprint(json.dumps({'detached': True}), flush=True)\n")
        output, error = self.finish(self.start("run", self.run_request("detached"), detached=True))
        self.assertEqual(error, "")
        self.assertTrue(next(e["details"]["value"]["detached"] for e in output if e["event"] == "process_stdout"))
        self.assertEqual(output[-1]["event"], "summary")

    def test_managed_sdk_default_interrupt_runs_finally_and_preserves_custom_handler(self):
        from test_stage1_cli import RouterProcess
        router = RouterProcess()
        self.addCleanup(router.close)
        for custom in (False, True):
            marker = self.directory / f"finally-{custom}.json"
            ready = self.directory / f"spinning-{custom}"
            self.entry("managed", f'''
import aos_sdk as aos
import json, signal
from pathlib import Path
target = getattr(signal, "SIGBREAK", signal.SIGINT)
def custom_handler(*args):
    raise KeyboardInterrupt
if {custom!r}:
    signal.signal(target, custom_handler)
context = aos.Context()
assert signal.getsignal(target) is (custom_handler if {custom!r} else signal.default_int_handler)
node = aos.Node("managed", context=context)
executor = aos.SingleThreadedExecutor(context=context)
executor.add_node(node)
Path({str(ready)!r}).write_text("ready")
try:
    executor.spin()
finally:
    stopped = executor.shutdown(timeout_sec=2)
    node.destroy_node()
    context.shutdown()
    Path({str(marker)!r}).write_text(json.dumps({{"finally": True, "executor_stopped": stopped}}))
''')
            process = self.start("run", self.run_request("managed",
                context={"domain_id": 74018, "zenoh_config": str(router.config)},
                shutdown={"sigint_timeout": 3, "sigterm_timeout": .1}))
            deadline = time.monotonic() + 5
            while not ready.exists() and process.poll() is None and time.monotonic() < deadline:
                time.sleep(.01)
            if not ready.exists():
                self.fail(str(process.communicate(timeout=5)))
            interrupt_process(process)
            output, _ = self.finish(process, 130)
            self.assertEqual(json.loads(marker.read_text()), {"finally": True, "executor_stopped": True})
            self.assertEqual(next(e["data"] for e in output if e["event"] == "summary")["cleanup_errors"], [])

    def test_uncooperative_tree_requires_forced_cleanup(self):
        marker = self.directory / "uncooperative.json"
        self.entry("uncooperative", f'''
import json, os, signal, subprocess, sys, time
from pathlib import Path
for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
    if hasattr(signal, name): signal.signal(getattr(signal, name), signal.SIG_IGN)
if len(sys.argv) > 1:
    Path({str(marker)!r}).write_text(json.dumps([os.getppid(), os.getpid()]))
else:
    subprocess.Popen([sys.executable, __file__, "child"])
while True: time.sleep(.02)
''')
        process = self.start("run", self.run_request("uncooperative",
            shutdown={"sigint_timeout": .1, "sigterm_timeout": .1}))
        deadline = time.monotonic() + 5
        while not marker.exists():
            self.assertLess(time.monotonic(), deadline)
            time.sleep(.01)
        identities = self.read_marker(marker)
        interrupt_process(process)
        output, _ = self.finish(process, 130)
        self.assertEqual(next(e["data"] for e in output if e["event"] == "summary")["cleanup_errors"], [])
        for pid in identities:
            self.assert_dead(pid)

    def test_native_full_stdout_cleanup_precedes_resumed_summary(self):
        marker = self.directory / "blocked-tree.json"
        self.entry("blocked-tree", f'''
import json, os, signal, subprocess, sys, time
from pathlib import Path
for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
    if hasattr(signal, name): signal.signal(getattr(signal, name), signal.SIG_IGN)
if len(sys.argv) > 1:
    Path({str(marker)!r}).write_text(json.dumps([os.getppid(), os.getpid()]))
    while True: time.sleep(.02)
subprocess.Popen([sys.executable, __file__, "child"])
while True: print(json.dumps({{"value": "x" * 4096}}), flush=True)
''')
        process = self.start("run", self.run_request("blocked-tree",
            shutdown={"sigint_timeout": .1, "sigterm_timeout": .1}))
        identities = self.read_marker(marker)
        time.sleep(.3)
        interrupt_process(process)
        for pid in identities:
            self.assert_dead(pid)
        self.assertIsNone(process.poll(), "the blocked output should still await a reader")
        output, _ = self.finish(process, 130)
        summary = next(e["data"] for e in output if e["event"] == "summary")
        self.assertEqual(summary["cleanup_errors"], [])

    def native_fixture(self, mode):
        source = Path(os.environ["AOS_PROCESS_FIXTURE"])
        target = self.prefix / "lib/demo/process_fixture.exe"
        shutil.copy2(source, target)
        self.manifest["entries"]["native"] = {"executable": target.relative_to(self.prefix).as_posix(), "kind": "aos_node"}
        self.save()
        marker = self.directory / (mode + "-pid")
        return marker, {"AOS_PROCESS_FIXTURE_MARKER": str(marker), "AOS_PROCESS_FIXTURE_MODE": mode}

    def test_native_tree_cleanup_after_leader_exit_and_controlled_stop(self):
        sentinel = subprocess.Popen([sys.executable, "-I", "-c", "import time; time.sleep(30)"],
                                    **console_process_options())
        self.addCleanup(lambda: (sentinel.kill(), sentinel.wait(5)) if sentinel.poll() is None else None)
        for mode in ("tree_exit", "tree"):
            marker, environment = self.native_fixture(mode)
            process = self.start("run", self.run_request("native", env=environment,
                shutdown={"sigint_timeout": .1, "sigterm_timeout": .1}))
            deadline = time.monotonic() + 5
            while not marker.exists():
                self.assertLess(time.monotonic(), deadline)
                time.sleep(.01)
            descendant = self.read_marker(marker, int)
            if mode == "tree":
                self.assertTrue(process_alive(descendant))
                interrupt_process(process)
            output, _ = self.finish(process, 130 if mode == "tree" else 0)
            summary = next(event["data"] for event in output if event["event"] == "summary")
            self.assertEqual(summary["cleanup_errors"], [])
            self.assert_dead(descendant)
            self.assertIsNone(sentinel.poll(), "cleanup touched an unrelated process")

    def test_native_spawn_failure_cleans_already_started_owned_job(self):
        marker, environment = self.native_fixture("hold")
        invalid = self.prefix / "lib/demo/invalid.exe"
        if os.name == "nt":
            shutil.copy2(os.environ["AOS_PROCESS_FIXTURE"], invalid)
        else:
            invalid.write_bytes(b"not an executable image")
        invalid.chmod(0o755)
        self.manifest["entries"]["invalid"] = {"executable": "lib/demo/invalid.exe", "kind": "aos_node"}
        self.save()
        plan = {"schema_version": 1, "shutdown": {"sigint_timeout": .1, "sigterm_timeout": .1},
            "processes": [{"id": "first", "package": "demo", "entry": "native", "env": environment},
                          {"id": "invalid", "package": "demo", "entry": "invalid", "args": ["x" * 40000] if os.name == "nt" else []}]}
        output, _ = self.finish(self.start("launch", {"prefixes": [str(self.prefix)], "plan": plan}), 1)
        started = [event["pid"] for event in output if event["event"] == "process_started"]
        self.assertEqual(len(started), 1)
        self.assertTrue(all(not process_alive(pid) for pid in started))
        summary = next(event["data"] for event in output if event["event"] == "summary")
        self.assertEqual(summary["cleanup_errors"], [])
        self.assertEqual(output[-1]["data"]["code"], "SPAWN_FAILED")


if __name__ == "__main__":
    unittest.main()
