"""Run managed work and query lifecycle state through a real Service.

Use an installed SDK interpreter. AOS_ZENOH_CONFIG or --config selects the
Zenoh session; the default expects the local router.
"""

import argparse
import json
from pathlib import Path
import threading

import aos_sdk as aos


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path)
    args = parser.parse_args()
    interface = aos.Interface.load(
        Path(__file__).resolve().parents[1] / "interfaces/String.json"
    )
    with aos.Context(config=args.config) as context:
        with (
            aos.LifecycleNode("worker", "/demo", context) as worker,
            aos.Node("observer", "/demo", context) as observer,
        ):
            publisher = worker.create_publisher(interface, "samples")
            received = []
            ready = threading.Event()

            def observe(message):
                received.append(message)
                ready.set()

            observer.create_subscription(interface, "samples", observe)
            state = observer.create_client(
                aos.lifecycle_service_interface("get_state"), "/demo/worker/get_state",
                qos_profile="lifecycle_services",
            )
            publisher.wait_for_subscribers(3)
            if not state.wait_for_service(3):
                raise RuntimeError("lifecycle state service was not discovered")
            blocked = False
            try:
                publisher.publish({"data": "before activation"})
            except aos.InactiveError:
                blocked = True
            executor = aos.MultiThreadedExecutor(2, context=context)
            executor.add_node(worker)
            executor.add_node(observer)
            spinner = threading.Thread(target=executor.spin, name="lifecycle-example")
            spinner.start()
            try:
                configured = worker.trigger_configure_async().result(3)
                activated = worker.trigger_activate_async().result(3)
                publisher.publish({"data": "managed work"})
                if not ready.wait(3):
                    raise RuntimeError("subscriber did not process the active sample")
                deactivated = worker.trigger_deactivate_async(drain_timeout=1).result(3)
                observed_state = state.call({}, 2)
                finalized = worker.trigger_shutdown_async(drain_timeout=1).result(3)
                print(json.dumps({
                    "inactive_publish_rejected": blocked,
                    "received": received,
                    "observed_state": observed_state,
                    "transitions": [configured, activated, deactivated, finalized],
                }, ensure_ascii=False))
            finally:
                stopped = executor.shutdown(timeout_sec=3)
                spinner.join(3)
                if not stopped or spinner.is_alive():
                    raise RuntimeError("executor still has running callbacks")


if __name__ == "__main__":
    main()
