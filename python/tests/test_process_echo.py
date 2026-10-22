"""T01: two independent installed-wheel processes exchange bytes via zenohd.

Install a freshly built wheel in a new venv, then run with that venv's Python:

    AOS_ZENOHD=/path/to/zenohd AOS_ECHO_RECORD=/path/to/t01.json \
      python -I -m unittest discover -s <repo>/python/tests -p test_process_echo.py -v

This acceptance test fails (does not skip) when AOS_ZENOHD is missing. Children
run with -I and verify the native extension is in their installed site-packages.
The optional JSON record includes commands, PIDs, import paths, exchanges and
exit status. Without AOS_ECHO_RECORD, the record is printed to stderr.
"""

import json
import os
import pathlib
import queue
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid

from process_support import assert_child_identity

ROOT = pathlib.Path(__file__).resolve().parents[2]
PAYLOADS = [b"hello from another Python process", b"", bytes([0xFF, 0x00, 0xFE])]


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def stop_process(process):
    """Clean up only our own child; return whether forced cleanup was needed."""
    if process is None or process.poll() is not None:
        return False
    if os.name == "nt":
        subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"],
                       capture_output=True, timeout=10, check=False)
    else:
        process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)
    return True


def read_events(stream, lines, messages):
    try:
        for line in stream:
            lines.append(line)
            messages.put(json.loads(line))
    except (ValueError, OSError) as error:
        messages.put({"event": "reader_error", "error": str(error)})
    finally:
        messages.put({"event": "eof"})
        stream.close()


class ProcessEchoTest(unittest.TestCase):
    def test_independent_native_processes_echo(self):
        zenohd = os.environ.get("AOS_ZENOHD")
        self.assertTrue(
            zenohd,
            "T01 requires AOS_ZENOHD=/absolute/path/to/zenohd (version 1.10.1)",
        )
        try:
            version = subprocess.run(
                [zenohd, "--version"], capture_output=True, text=True, timeout=5,
                check=True,
            ).stdout.strip()
        except (OSError, subprocess.SubprocessError) as error:
            self.fail(f"cannot execute AOS_ZENOHD={zenohd!r}: {error}")
        self.assertRegex(version, r"\bzenohd v?1\.10\.1\b")

        record = {"test": "T01", "status": "failed", "router_version": version}
        router = provider = client = None
        reader = None
        provider_lines = []
        with tempfile.TemporaryDirectory(prefix="aos-t01-") as directory:
            temp = pathlib.Path(directory)
            port = free_port()
            endpoint = f"tcp/127.0.0.1:{port}"
            config = temp / "session.json5"
            # Exercise the shipped peer/loopback/gossip configuration, changing
            # only its router endpoint. Discovery may establish direct peers.
            template = ROOT / "config/zenoh-session.json5"
            session_config = template.read_text()
            self.assertEqual(session_config.count("tcp/localhost:7447"), 1)
            session_config = session_config.replace("tcp/localhost:7447", endpoint)
            config.write_text(session_config)
            router_command = [
                zenohd, "--config", str(ROOT / "config/zenoh-router.json5"),
                f'--cfg=listen/endpoints:["{endpoint}"]',
            ]
            worker_command = [
                sys.executable, "-I", str(ROOT / "examples/raw_echo.py"),
            ]
            common = [
                "--config", str(config),
                "--topic-prefix", f"/aos_test/echo_{uuid.uuid4().hex}",
                "--domain-id", "31",
            ]
            provider_command = worker_command + ["provider", *common,
                                                "--timeout", "30", "--stop-on-stdin"]
            client_command = worker_command + ["client", *common, "--timeout", "5"]
            for payload in PAYLOADS:
                client_command += ["--hex", payload.hex()]
            record.update({
                "endpoint": endpoint,
                "session_config_template": str(template),
                "session_config": session_config,
                "commands": {"router": router_command, "provider": provider_command,
                             "client": client_command},
                "payloads_hex": [payload.hex() for payload in PAYLOADS],
            })
            with (temp / "router.log").open("w+") as router_log, (
                temp / "provider.log"
            ).open("w+") as provider_log, (temp / "client.log").open("w+") as client_log:
                try:
                    router = subprocess.Popen(
                        router_command, stdout=router_log, stderr=subprocess.STDOUT,
                    )
                    record["router_pid"] = router.pid
                    deadline = time.monotonic() + 10
                    while True:
                        if router.poll() is not None:
                            self.fail(f"zenohd exited with {router.returncode}")
                        try:
                            socket.create_connection(("127.0.0.1", port), 0.1).close()
                            break
                        except OSError:
                            if time.monotonic() >= deadline:
                                self.fail(f"zenohd did not listen on {endpoint}")
                            time.sleep(0.02)
                    messages = queue.Queue()
                    provider = subprocess.Popen(
                        provider_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                        stderr=provider_log, text=True,
                    )
                    reader = threading.Thread(
                        target=read_events,
                        args=(provider.stdout, provider_lines, messages), daemon=True,
                    )
                    reader.start()
                    ready = messages.get(timeout=10)
                    self.assertEqual(ready["event"], "ready", ready)
                    assert_child_identity(self, ready, provider)
                    client = subprocess.Popen(
                        client_command, stdout=subprocess.PIPE, stderr=client_log,
                        text=True,
                    )
                    client_stdout, _ = client.communicate(timeout=20)
                    record["client_stdout"] = client_stdout
                    self.assertEqual(client.returncode, 0, client_stdout)
                    client_events = [json.loads(line) for line in client_stdout.splitlines()]
                    self.assertEqual([event["event"] for event in client_events],
                                     ["ready", "done"])
                    client_done = client_events[-1]
                    assert_child_identity(self, client_done, client)
                    self.assertEqual(client_done["pid"], client_events[0]["pid"])
                    self.assertNotEqual(provider.pid, client.pid)
                    self.assertNotIn(os.getpid(), [provider.pid, client.pid])
                    self.assertEqual(
                        [(item["sent_hex"], item["received_hex"])
                         for item in client_done["exchanges"]],
                        [(payload.hex(), payload.hex()) for payload in PAYLOADS],
                    )
                    provider.stdin.write("stop\n")
                    provider.stdin.flush()
                    provider.stdin.close()
                    provider.wait(timeout=10)
                    reader.join(timeout=2)
                    self.assertFalse(reader.is_alive(), "provider stdout did not close")
                    self.assertEqual(provider.returncode, 0)
                    provider_events = [json.loads(line) for line in provider_lines]
                    provider_done = provider_events[-1]
                    self.assertEqual(provider_done["event"], "done")
                    self.assertEqual(provider_done["pid"], ready["pid"])
                    received = [event["payload_hex"] for event in provider_events
                                if event["event"] == "received"]
                    for payload in PAYLOADS:
                        self.assertIn(payload.hex(), received)
                    for child in (provider_done, client_done):
                        self.assertTrue(child["isolated"])
                        self.assertTrue(child["context_closed"])
                    self.assertEqual(provider_done["native_path"], client_done["native_path"])
                    self.assertIsNone(router.poll(), "router exited during the exchange")
                    record.update(status="passed", provider=provider_done, client=client_done)
                finally:
                    record["forced_python_cleanup"] = {
                        "provider": stop_process(provider),
                        "client": stop_process(client),
                    }
                    if reader is not None:
                        reader.join(timeout=2)
                    if router is not None and router.poll() is None:
                        if os.name == "posix":
                            router.send_signal(signal.SIGINT)
                        else:
                            router.terminate()
                        try:
                            router.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            stop_process(router)
                    record["provider_stdout"] = "".join(provider_lines)
                    record["exit_codes"] = {
                        "router": None if router is None else router.returncode,
                        "provider": None if provider is None else provider.returncode,
                        "client": None if client is None else client.returncode,
                    }
                    for name, log in (("router", router_log), ("provider", provider_log),
                                      ("client", client_log)):
                        log.seek(0)
                        record[name + "_stderr"] = log.read()
                    serialized = json.dumps(record, indent=2) + "\n"
                    destination = os.environ.get("AOS_ECHO_RECORD")
                    if destination:
                        pathlib.Path(destination).write_text(serialized)
                        sys.stderr.write(f"T01 {record['status']}: {destination}\n")
                    else:
                        sys.stderr.write(serialized)


if __name__ == "__main__":
    unittest.main()
