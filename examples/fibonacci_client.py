"""An Action caller: python -m examples.fibonacci_client 8."""

import argparse
import json
from pathlib import Path
import time

from aos_sdk import ActionClient, Context, Interface, Node, SingleThreadedExecutor


def main():
    parser = argparse.ArgumentParser(description="Send one goal and print its protocol responses.")
    parser.add_argument("order", type=int)
    parser.add_argument("--config")
    parser.add_argument("--action", default="/fibonacci")
    parser.add_argument("--timeout", type=float, default=10)
    parser.add_argument("--cancel-after", type=float)
    args = parser.parse_args()
    interface = Interface.load(
        Path(__file__).parents[1] / "interfaces/action/Fibonacci.json"
    )

    def feedback(message):
        print(json.dumps({"event": "feedback", **message}), flush=True)

    with Context(config=args.config) as context, Node(
        "fibonacci_caller", context=context
    ) as node, SingleThreadedExecutor(context=context) as executor:
        client = ActionClient(node, interface, args.action)
        executor.add_node(node)
        if not client.wait_for_server(timeout_sec=args.timeout):
            raise TimeoutError(f"Action {args.action} was not ready within {args.timeout}s")
        pending = client.send_goal_async(
            {"order": args.order}, feedback_callback=feedback, timeout=args.timeout,
        )
        executor.spin_until_future_complete(pending, timeout_sec=args.timeout + 1)
        goal = pending.result(0)
        print(json.dumps({
            "event": "goal_response", "goal_id": goal.goal_id,
            "accepted": goal.accepted, "stamp": goal.stamp,
        }), flush=True)
        if not goal.accepted:
            return
        # Register the result wait before requesting cancellation.
        result = goal.get_result_async(timeout=args.timeout)
        cancel_at = None if args.cancel_after is None else time.monotonic() + args.cancel_after
        while not result.done():
            executor.spin_once(timeout_sec=0.02)
            if cancel_at is not None and time.monotonic() >= cancel_at:
                canceled = goal.cancel_goal_async(timeout=args.timeout)
                executor.spin_until_future_complete(canceled, timeout_sec=args.timeout + 1)
                print(json.dumps({
                    "event": "cancel_response", **canceled.result(0),
                }), flush=True)
                cancel_at = None
        print(json.dumps({
            "event": "result", "goal_id": goal.goal_id, **result.result(0),
        }), flush=True)


if __name__ == "__main__":
    main()
