"""Installed consumer: submit one batch once, then observe provider restarts."""
import argparse
import json
import os
import signal
import threading

import aos_sdk as aos


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--labels", type=int, default=3)
    args = parser.parse_args()
    if not 1 <= args.labels <= 100:
        parser.error("--labels must be in 1..100")
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, lambda *_: stop.set())
    labels = aos.Interface.load(aos.get_package_resource("aos_demo", "label"))
    status = aos.Interface.load(aos.get_package_resource("aos_demo", "status"))
    observed = {"messages": 0, "last_state": None, "provider_pid": None}
    with aos.Context() as context:
        with aos.Node(
            "consumer", context=context,
            summary="Submits one label batch once and reports status changes; never replays a batch after provider restart.",
        ) as node:
            publisher = node.create_publisher(labels, "labels")

            def receive(value):
                observed["messages"] += 1
                if (value["state"], value["pid"]) != (
                    observed["last_state"], observed["provider_pid"],
                ):
                    print(json.dumps({
                        "event": "business_state", "message": value, "pid": os.getpid(),
                    }), flush=True)
                observed["last_state"] = value["state"]
                observed["provider_pid"] = value["pid"]

            node.create_subscription(status, "status", receive)
            executor = aos.MultiThreadedExecutor(2, context=context)
            executor.add_node(node)
            spinner = threading.Thread(target=executor.spin, name="consumer-executor")
            spinner.start()
            try:
                publisher.wait_for_subscribers(5)
                for number in range(args.labels):
                    publisher.publish({"label": f"installed-demo-{number + 1}"})
                print(json.dumps({
                    "event": "batch_submitted", "count": args.labels, "pid": os.getpid(),
                    "node": node.fully_qualified_name,
                    "effective_config": node.effective_config,
                }), flush=True)
                while not stop.wait(.1):
                    pass  # Re-enter Python so Windows console handlers can run.
            finally:
                stopped = executor.shutdown(timeout_sec=5)
                spinner.join(5)
                print(json.dumps({
                    "event": "program_stopped", "pid": os.getpid(), "observed": observed,
                    "executor_stopped": stopped and not spinner.is_alive(),
                }), flush=True)
                if not stopped or spinner.is_alive():
                    raise RuntimeError("consumer executor did not stop")


if __name__ == "__main__":
    main()
