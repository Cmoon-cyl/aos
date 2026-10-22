"""Echo raw bytes between two independent installed-SDK Python processes.

Start zenohd with config/zenoh-router.json5, then run in separate terminals:

    python -I examples/raw_echo.py provider --config config/zenoh-session.json5
    python -I examples/raw_echo.py client --config config/zenoh-session.json5

The provider exits on Ctrl-C or after --timeout seconds. The client retries only
this idempotent echo fixture while volatile subscriptions are being discovered.
This is a raw Topic example, not the Service API.
"""

import argparse
import importlib.machinery
import importlib.metadata
import json
import os
import pathlib
import sys
import sysconfig
import threading
import time

import aos_sdk
from aos_sdk import _native


def emit(event, **fields):
    print(json.dumps({"event": event, **fields}), flush=True)


def native_identity():
    native_path = pathlib.Path(_native.__file__).resolve()
    package_path = pathlib.Path(aos_sdk.__file__).resolve()
    site_packages = pathlib.Path(sysconfig.get_path("platlib")).resolve()
    if not isinstance(_native.__loader__, importlib.machinery.ExtensionFileLoader):
        raise RuntimeError("aos_sdk._native is not a native extension")
    if not native_path.is_relative_to(site_packages / "aos_sdk"):
        raise RuntimeError(f"native module is not installed under {site_packages}")
    if package_path.parent != native_path.parent:
        raise RuntimeError("Python package and native module came from different locations")
    return {
        "pid": os.getpid(),
        "ppid": os.getppid(),
        "executable": sys.executable,
        "isolated": bool(sys.flags.isolated),
        "native_path": str(native_path),
        "package_path": str(package_path),
        "version": importlib.metadata.version("aos-sdk"),
    }


def provide(subscription, publisher, timeout, stop_on_stdin):
    stop = threading.Event()
    if stop_on_stdin:
        def read_stop():
            # Test orchestration only. Requests and responses always use Zenoh.
            if sys.stdin.readline().strip() == "stop":
                stop.set()

        threading.Thread(target=read_stop, daemon=True).start()
    deadline = time.monotonic() + timeout
    received = 0
    while not stop.is_set():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            if stop_on_stdin:
                raise TimeoutError("provider timed out before its normal stop command")
            break
        try:
            payload = subscription.recv(min(0.1, remaining))
        except aos_sdk.WaitTimeoutError:
            continue
        received += 1
        emit("received", payload_hex=payload.hex())
        publisher.publish(payload)
    return {"received_count": received}


def request(subscription, publisher, timeout, payloads):
    exchanges = []
    for payload in payloads:
        deadline = time.monotonic() + timeout
        attempts = 0
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"no echo for {payload.hex()!r} within {timeout}s")
            attempts += 1
            publisher.publish(payload)
            try:
                response = subscription.recv(min(0.1, remaining))
            except aos_sdk.WaitTimeoutError:
                continue
            # A retried earlier request can leave a late duplicate response.
            if response == payload:
                exchanges.append({
                    "sent_hex": payload.hex(),
                    "received_hex": response.hex(),
                    "attempts": attempts,
                })
                break
    return {"exchanges": exchanges}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("role", choices=("provider", "client"))
    parser.add_argument("--config", type=pathlib.Path, required=True)
    parser.add_argument("--topic-prefix", default="/aos_example/echo")
    parser.add_argument("--domain-id", type=int, default=0)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--hex", action="append", dest="payloads",
                        help="request bytes in hexadecimal; repeat for multiple requests")
    parser.add_argument("--stop-on-stdin", action="store_true",
                        help="provider test mode: stop when stdin receives 'stop'")
    args = parser.parse_args()
    if not 0 < args.timeout < float("inf"):
        parser.error("--timeout must be finite and positive")
    identity = native_identity()
    payloads = [bytes.fromhex(value) for value in args.payloads or ["68656c6c6f"]]
    prefix = args.topic_prefix.rstrip("/")
    with aos_sdk.Context(config=args.config, domain_id=args.domain_id) as ctx:
        if args.role == "provider":
            subscription = ctx.create_subscription(prefix + "/request")
            publisher = ctx.create_publisher(prefix + "/response")
        else:
            subscription = ctx.create_subscription(prefix + "/response")
            publisher = ctx.create_publisher(prefix + "/request")
        emit("ready", role=args.role, **identity)
        try:
            if args.role == "provider":
                result = provide(subscription, publisher, args.timeout, args.stop_on_stdin)
            else:
                result = request(subscription, publisher, args.timeout, payloads)
        except KeyboardInterrupt:
            if args.role != "provider":
                raise
            result = {"interrupted": True}
    if ctx.ok():
        raise RuntimeError("Context is still open after shutdown")
    emit("done", role=args.role, context_closed=True, **identity, **result)


if __name__ == "__main__":
    main()
