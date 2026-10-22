"""Run with: python -m examples.add_two_ints_client 2 3 --config <session.json5>."""

import argparse
import json
from pathlib import Path

from aos_sdk import Context, Interface, Node, SingleThreadedExecutor


def main():
    parser = argparse.ArgumentParser(description="Call AddTwoInts once; no retries.")
    parser.add_argument("a", type=int)
    parser.add_argument("b", type=int)
    parser.add_argument("--config")
    parser.add_argument("--service", default="/add_two_ints")
    parser.add_argument("--timeout", type=float, default=10)
    args = parser.parse_args()
    interface = Interface.load(Path(__file__).parents[1] / "interfaces/AddTwoInts.json")
    with Context(config=args.config) as context, Node(
        "calculator_client", context=context
    ) as node, SingleThreadedExecutor(context=context) as executor:
        client = node.create_client(interface, args.service)
        executor.add_node(node)
        if not client.wait_for_service(timeout_sec=args.timeout):
            raise TimeoutError(f"Service {args.service} was not ready within {args.timeout}s")
        future = client.call_async({"a": args.a, "b": args.b}, timeout=args.timeout)
        executor.spin_until_future_complete(future, timeout_sec=args.timeout + 1)
        print(json.dumps(future.result(timeout=0)), flush=True)


if __name__ == "__main__":
    main()
