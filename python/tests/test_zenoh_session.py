"""Installed-wheel Context over real Zenoh sessions and an upstream zenohd.

Needs a zenohd 1.10.1 executable (see config/README.md); skipped unless set:

    AOS_ZENOHD=/path/to/zenohd python -I -m unittest discover -s <repo>/python/tests

Each test process opens its own sessions through the local router. This checks
the binding's session lifecycle; the two-process echo is a separate check.
"""

import gc
import json
import os
import pathlib
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

import aos_sdk

ZENOHD = os.environ.get("AOS_ZENOHD")
CONFIG_DIR = pathlib.Path(__file__).resolve().parents[2] / "config"
DEFAULT_ROUTER = "tcp/localhost:7447"

router = None
session_config = None
_tmp = None


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def write_session_config(directory, endpoint):
    """The checked-in peer config, pointed at `endpoint`."""
    text = (CONFIG_DIR / "zenoh-session.json5").read_text()
    if text.count(DEFAULT_ROUTER) != 1:
        raise AssertionError("session config must name the default router once")
    path = pathlib.Path(directory) / f"session-{endpoint.rsplit(':', 1)[1]}.json5"
    path.write_text(text.replace(DEFAULT_ROUTER, endpoint))
    return path


def report(name, seconds):
    sys.stderr.write(f"[timing] {name}: {seconds * 1000:.1f} ms\n")


def setUpModule():
    global router, session_config, _tmp
    if not ZENOHD:
        raise unittest.SkipTest("AOS_ZENOHD is not set")
    _tmp = tempfile.TemporaryDirectory()
    port = free_port()
    endpoint = f"tcp/127.0.0.1:{port}"
    log = open(pathlib.Path(_tmp.name) / "zenohd.log", "wb")
    router = subprocess.Popen(
        [
            ZENOHD,
            "--config",
            str(CONFIG_DIR / "zenoh-router.json5"),
            f'--cfg=listen/endpoints:["{endpoint}"]',
        ],
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    log.close()
    deadline = time.monotonic() + 10
    while True:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            break
        except OSError:
            if router.poll() is not None or time.monotonic() > deadline:
                tearDownModule()
                raise RuntimeError(f"zenohd did not listen on {endpoint}")
            time.sleep(0.05)
    session_config = write_session_config(_tmp.name, endpoint)


def tearDownModule():
    # Stop only the router this module started, by its own PID.
    if router is not None and router.poll() is None:
        if os.name == "posix":
            router.send_signal(signal.SIGINT)
        else:
            # zenohd is test infrastructure, not an SDK process under test.
            router.terminate()
        try:
            router.wait(timeout=10)
        except subprocess.TimeoutExpired:
            router.kill()
            router.wait()
    if _tmp is not None:
        _tmp.cleanup()


def context(**kwargs):
    return aos_sdk.Context(config=session_config, **kwargs)


def publish_until_received(publisher, subscription, payload, limit=5.0):
    """Volatile samples sent before the subscription is matched are lost by
    design, so keep publishing until one arrives."""
    deadline = time.monotonic() + limit
    while time.monotonic() < deadline:
        publisher.publish(payload)
        try:
            return subscription.recv(0.1)
        except aos_sdk.WaitTimeoutError:
            pass
    return None


def drain(subscription):
    while True:
        try:
            subscription.recv(0.2)
        except aos_sdk.WaitTimeoutError:
            return


class SessionTest(unittest.TestCase):
    def test_two_contexts_exchange_bytes(self):
        with context() as talker, context() as listener:
            subscription = listener.create_subscription("/aos_test/chatter")
            publisher = talker.create_publisher("/aos_test/chatter")
            for payload in (b'{"data":"hi"}', b"", bytes([0xFF, 0x00, 0xFE])):
                with self.subTest(payload=payload):
                    self.assertEqual(
                        publish_until_received(publisher, subscription, payload), payload
                    )
                    drain(subscription)
            self.assertTrue(talker.ok())
        self.assertFalse(talker.ok())
        self.assertFalse(listener.ok())

    def test_domains_are_isolated(self):
        with context(domain_id=1) as talker, context(domain_id=1) as same, context(
            domain_id=2
        ) as other:
            in_domain = same.create_subscription("/aos_test/domain")
            out_of_domain = other.create_subscription("/aos_test/domain")
            publisher = talker.create_publisher("/aos_test/domain")
            # The same-domain receive proves the samples were on the wire.
            self.assertEqual(publish_until_received(publisher, in_domain, b"x"), b"x")
            for _ in range(5):
                publisher.publish(b"x")
            with self.assertRaises(aos_sdk.WaitTimeoutError):
                out_of_domain.recv(0.5)

    def test_domain_id_defaults_to_environment(self):
        old = os.environ.get("AOS_DOMAIN_ID")
        try:
            os.environ["AOS_DOMAIN_ID"] = "7"
            with context() as from_env, context(domain_id=7) as explicit:
                subscription = explicit.create_subscription("/aos_test/env")
                publisher = from_env.create_publisher("/aos_test/env")
                self.assertEqual(publish_until_received(publisher, subscription, b"7"), b"7")
            for value in ("abc", "-1", "4294967296"):
                os.environ["AOS_DOMAIN_ID"] = value
                with self.subTest(value=value), self.assertRaises(
                    aos_sdk.InvalidArgumentError
                ):
                    context()
        finally:
            if old is None:
                os.environ.pop("AOS_DOMAIN_ID", None)
            else:
                os.environ["AOS_DOMAIN_ID"] = old

    def test_invalid_arguments(self):
        for domain_id in (-1, 2**32):
            with self.subTest(domain_id=domain_id), self.assertRaises(
                aos_sdk.InvalidArgumentError
            ):
                context(domain_id=domain_id)
        with self.assertRaises(aos_sdk.InvalidArgumentError):
            aos_sdk.Context(config=pathlib.Path(_tmp.name) / "missing.json5")
        with self.assertRaises(TypeError):
            aos_sdk.Context(0)
        with context() as ctx, self.assertRaises(aos_sdk.InvalidArgumentError):
            ctx.create_publisher("relative")

    def test_missing_router_fails_with_communication_error_without_holding_gil(self):
        with tempfile.TemporaryDirectory() as directory:
            config = write_session_config(directory, f"tcp/127.0.0.1:{free_port()}")
            # The new default permits late Router startup. This explicit
            # fail-fast profile retains the blocking-open/GIL regression check.
            config.write_text(json.dumps({
                "mode": "client", "listen": {"endpoints": []},
                "connect": {"endpoints": [f"tcp/127.0.0.1:{free_port()}"],
                            "timeout_ms": 3000, "exit_on_failure": True},
                "scouting": {"multicast": {"enabled": False}, "gossip": {"enabled": False}},
            }))
            ticks = 0
            stop = threading.Event()

            def tick():
                nonlocal ticks
                while not stop.is_set():
                    ticks += 1
                    time.sleep(0.01)

            ticker = threading.Thread(target=tick)
            ticker.start()
            started = time.monotonic()
            try:
                with self.assertRaises(aos_sdk.CommunicationError):
                    aos_sdk.Context(config=config)
            finally:
                elapsed = time.monotonic() - started
                stop.set()
                ticker.join()
        report("open without router", elapsed)
        self.assertLess(elapsed, 10)
        # About 300 ticks fit in the 3 s connect window; none if open held the GIL.
        self.assertGreater(ticks, 50)

    def test_shutdown_wakes_blocked_recv(self):
        ctx = context()
        subscription = ctx.create_subscription("/aos_test/blocked")
        outcome = {}

        def receive():
            started = time.monotonic()
            try:
                subscription.recv(30)
            except aos_sdk.AOSError as error:
                outcome["error"] = error
            outcome["elapsed"] = time.monotonic() - started

        receiver = threading.Thread(target=receive)
        receiver.start()
        # This thread keeps running Python while the receiver waits.
        time.sleep(0.3)
        started = time.monotonic()
        ctx.shutdown()
        report("Context.shutdown with a blocked recv", time.monotonic() - started)
        receiver.join(10)
        self.assertFalse(receiver.is_alive())
        self.assertIsInstance(outcome["error"], aos_sdk.InvalidStateError)
        self.assertLess(outcome["elapsed"], 10)
        with self.assertRaises(aos_sdk.InvalidStateError):
            subscription.recv(0)

    def test_destroy_wakes_only_that_subscription(self):
        with context() as talker, context() as listener:
            destroyed = listener.create_subscription("/aos_test/destroy")
            kept = listener.create_subscription("/aos_test/destroy")
            publisher = talker.create_publisher("/aos_test/destroy")
            outcome = {}

            def receive():
                try:
                    destroyed.recv(30)
                except aos_sdk.AOSError as error:
                    outcome["error"] = error

            receiver = threading.Thread(target=receive)
            receiver.start()
            time.sleep(0.3)
            started = time.monotonic()
            destroyed.destroy()
            report("Subscription.destroy with a blocked recv", time.monotonic() - started)
            receiver.join(10)
            self.assertIsInstance(outcome["error"], aos_sdk.InvalidStateError)
            self.assertEqual(publish_until_received(publisher, kept, b"k"), b"k")
            with self.assertRaises(aos_sdk.InvalidStateError):
                destroyed.destroy()

    def test_concurrent_shutdown_has_one_winner(self):
        ctx = context()
        publisher = ctx.create_publisher("/aos_test/race")
        barrier = threading.Barrier(8)
        results = []
        lock = threading.Lock()

        def shutdown():
            barrier.wait()
            try:
                ctx.shutdown()
                result = "ok"
            except aos_sdk.InvalidStateError:
                result = "closed"
            with lock:
                results.append(result)

        threads = [threading.Thread(target=shutdown) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)
        self.assertEqual(sorted(results), ["closed"] * 7 + ["ok"])
        self.assertFalse(ctx.ok())
        with self.assertRaises(aos_sdk.InvalidStateError):
            publisher.publish(b"late")
        with self.assertRaises(aos_sdk.InvalidStateError):
            ctx.create_subscription("/aos_test/race")

    def test_drop_releases_session_without_explicit_cleanup(self):
        # Drop runs with the GIL held; record how long it blocks other threads.
        for round_ in range(3):
            ctx = context()
            publisher = ctx.create_publisher("/aos_test/drop")
            subscription = ctx.create_subscription("/aos_test/drop")
            started = time.monotonic()
            del publisher
            report(f"drop Publisher (round {round_})", time.monotonic() - started)
            started = time.monotonic()
            del subscription
            report(f"drop Subscription (round {round_})", time.monotonic() - started)
            started = time.monotonic()
            del ctx
            gc.collect()
            elapsed = time.monotonic() - started
            report(f"drop last Context reference (round {round_})", elapsed)
            self.assertLess(elapsed, 2)

        # Entities keep the session alive after the Python Context is gone.
        ctx = context()
        subscription = ctx.create_subscription("/aos_test/drop")
        publisher = ctx.create_publisher("/aos_test/drop")
        del ctx
        gc.collect()
        self.assertEqual(publish_until_received(publisher, subscription, b"d"), b"d")
        started = time.monotonic()
        del publisher, subscription
        gc.collect()
        report("drop last entities owning the session", time.monotonic() - started)


class DefaultConfigTest(unittest.TestCase):
    def test_default_config_opens_locally_before_the_router(self):
        # The embedded peer opens and works locally while reconnecting.
        host, port = DEFAULT_ROUTER.removeprefix("tcp/").rsplit(":", 1)
        try:
            socket.create_connection((host, int(port)), timeout=0.2).close()
        except OSError:
            pass
        else:
            self.skipTest(f"{DEFAULT_ROUTER} is in use")
        started = time.monotonic()
        with aos_sdk.Context() as context:
            publisher = context.create_publisher("/default/late_router")
            subscription = context.create_subscription("/default/late_router")
            self.assertEqual(publish_until_received(publisher, subscription, b"local"), b"local")
        report("open default config without router", time.monotonic() - started)
        self.assertLess(time.monotonic() - started, 5)


class ProcessExitTest(unittest.TestCase):
    """Interpreter exit with live sessions must not hang or write to stdout."""

    SCRIPT = """
import sys, time, aos_sdk
ctx = aos_sdk.Context(config=sys.argv[1])
subscription = ctx.create_subscription("/aos_test/exit")
publisher = ctx.create_publisher("/aos_test/exit")
deadline = time.monotonic() + 5
while time.monotonic() < deadline:
    publisher.publish(b"e")
    try:
        assert subscription.recv(0.1) == b"e"
        break
    except aos_sdk.WaitTimeoutError:
        pass
else:
    sys.exit("no echo")
if sys.argv[2] == "shutdown":
    ctx.shutdown()
print("done")
"""

    def run_script(self, mode):
        env = dict(os.environ, RUST_LOG="trace")
        started = time.monotonic()
        result = subprocess.run(
            [sys.executable, "-I", "-c", self.SCRIPT, str(session_config), mode],
            capture_output=True,
            env=env,
            timeout=30,
        )
        report(f"child process with {mode}", time.monotonic() - started)
        sys.stderr.write(
            f"[child {mode}] exit={result.returncode} "
            f"stdout={result.stdout!r} stderr={result.stderr[-2000:]!r}\n"
        )
        return result

    def test_exit_after_explicit_shutdown(self):
        result = self.run_script("shutdown")
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout.splitlines(), [b"done"])

    def test_exit_with_open_context(self):
        result = self.run_script("keep")
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout.splitlines(), [b"done"])


if __name__ == "__main__":
    unittest.main()
