"""Installed, lifecycle-managed workcell; launch supplies its deployment."""
import json
import os
import signal
import threading

import aos_sdk as aos


def main():
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, lambda *_: stop.set())
    labels = aos.Interface.load(aos.get_package_resource("aos_demo", "label"))
    status = aos.Interface.load(aos.get_package_resource("aos_demo", "status"))
    state = {"received": 0, "last_label": "", "sequence": 0}
    lock = threading.Lock()
    with aos.Context() as context:
        with aos.LifecycleNode(
            "worker", context=context,
            summary=(
                "Managed label workcell. Set threshold while inactive, then activate. "
                "Send labels to the labels Topic; status reports ready after threshold "
                "labels are received. Each process restart starts a new empty workcell."
            ),
        ) as node:
            node.declare_parameter(
                "threshold", 3,
                aos.ParameterDescriptor(
                    description="Number of labels needed before the workcell reports ready.",
                    integer_range=aos.IntegerRange(1, 100),
                    allowed_states=("inactive",),
                ),
            )
            publisher = node.create_publisher(status, "status")

            def receive(value):
                with lock:
                    state["received"] += 1
                    state["last_label"] = value["label"]
                    print(json.dumps({
                        "event": "label_received", "count": state["received"],
                        "label": value["label"], "pid": os.getpid(),
                    }), flush=True)

            def tick():
                threshold = node.get_parameter("threshold").value
                with lock:
                    state["sequence"] += 1
                    value = {
                        **state, "threshold": threshold,
                        "state": "ready" if state["received"] >= threshold else "waiting",
                        "pid": os.getpid(),
                    }
                try:
                    publisher.publish(value)
                except aos.InactiveError:
                    # A transition can close admission after this timer starts.
                    # Skip this publication without retrying business work.
                    print(json.dumps({
                        "event": "status_publication_skipped",
                        "reason": "lifecycle_inactive", "pid": os.getpid(),
                    }), flush=True)

            node.create_subscription(labels, "labels", receive)
            node.create_timer(0.05, tick)
            executor = aos.MultiThreadedExecutor(3, context=context)
            executor.add_node(node)
            callback_errors = []

            def spin():
                try:
                    executor.spin()
                except Exception as error:
                    callback_errors.append(error)
                    print(json.dumps({
                        "event": "program_callback_failed", "pid": os.getpid(),
                        "error": f"{type(error).__name__}: {error}",
                    }), flush=True)
                    stop.set()

            spinner = threading.Thread(target=spin, name="workcell-executor")
            spinner.start()
            print(json.dumps({
                "event": "program_initialized", "pid": os.getpid(),
                "node": node.fully_qualified_name,
                "lifecycle": node.get_current_state().label,
                "effective_config": node.effective_config,
            }), flush=True)
            try:
                while not stop.wait(.1):
                    pass  # Re-enter Python so Windows console handlers can run.
            finally:
                stopped = executor.shutdown(timeout_sec=5)
                spinner.join(5)
                print(json.dumps({
                    "event": "program_stopped", "pid": os.getpid(),
                    "state": state, "executor_stopped": stopped and not spinner.is_alive(),
                }), flush=True)
                if not stopped or spinner.is_alive():
                    raise RuntimeError("workcell executor did not stop")
            if callback_errors:
                raise RuntimeError("workcell callback failed") from callback_errors[0]


if __name__ == "__main__":
    main()
