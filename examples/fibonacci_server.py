"""A cancellable Action provider: python -m examples.fibonacci_server."""

import argparse
import json
from pathlib import Path
import sys
import threading
import time

from aos_sdk import (
    ActionServer, CancelResponse, Context, Interface, MultiThreadedExecutor,
    Node, ReentrantCallbackGroup,
)


def main():
    parser = argparse.ArgumentParser(description="Compute Fibonacci numbers with feedback.")
    parser.add_argument("--config")
    parser.add_argument("--action", default="/fibonacci")
    parser.add_argument("--delay", type=float, default=0.1)
    parser.add_argument("--stop-on-stdin", action="store_true",
                        help="stop normally when stdin receives 'stop'")
    args = parser.parse_args()
    interface = Interface.load(
        Path(__file__).parents[1] / "interfaces/action/Fibonacci.json"
    )

    def execute(goal):
        sequence = []
        print(json.dumps({"event": "execute", "goal_id": goal.goal_id}), flush=True)
        for index in range(goal.request["order"]):
            if goal.is_cancel_requested:
                goal.canceled()
                print(json.dumps({
                    "event": "completed", "goal_id": goal.goal_id,
                    "status": goal.status, "sequence": sequence,
                }), flush=True)
                return {"sequence": sequence}
            sequence.append(index if index < 2 else sequence[-1] + sequence[-2])
            goal.publish_feedback({"partial_sequence": list(sequence)})
            time.sleep(args.delay)
        goal.succeed()
        print(json.dumps({
            "event": "completed", "goal_id": goal.goal_id,
            "status": goal.status, "sequence": sequence,
        }), flush=True)
        return {"sequence": sequence}

    with Context(config=args.config) as context, Node(
        "fibonacci_provider", context=context,
        summary="Computes Fibonacci numbers and supports cooperative cancellation.",
        tags=["math", "example"],
    ) as node, MultiThreadedExecutor(4, context=context) as executor:
        ActionServer(
            node, interface, args.action, execute,
            callback_group=ReentrantCallbackGroup(),
            cancel_callback=lambda goal: CancelResponse.ACCEPT,
            summary="Compute a finite Fibonacci sequence with progress feedback.",
            side_effects="read_only", contract_version="1",
        )
        executor.add_node(node)
        stop = threading.Event()
        if args.stop_on_stdin:
            def read_stop():
                for line in sys.stdin:
                    if line.strip() == "stop":
                        stop.set()
                        return
            threading.Thread(target=read_stop, daemon=True).start()
        print(json.dumps({"event": "ready", "action": args.action}), flush=True)
        try:
            if args.stop_on_stdin:
                while context.ok() and not stop.is_set():
                    executor.spin_once(timeout_sec=0.1)
            else:
                executor.spin()
        except KeyboardInterrupt:
            pass
        finally:
            # Closing the Context wakes native waits. Python callbacks remain
            # cooperative; the Executor context manager then waits for them.
            if context.ok():
                context.shutdown()


if __name__ == "__main__":
    main()
