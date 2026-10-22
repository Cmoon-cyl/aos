"""Run with: python -m examples.typed_listener --config <session.json5>."""

import argparse
import json
from pathlib import Path

from aos_sdk import Context, DurabilityPolicy, Interface, Node, QoSProfile


def main():
    parser = argparse.ArgumentParser(description="Receive typed JSON messages.")
    parser.add_argument("--config")
    parser.add_argument("--topic", default="/chatter")
    parser.add_argument("--count", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=10)
    parser.add_argument("--transient-local", action="store_true")
    args = parser.parse_args()
    interface = Interface.load(Path(__file__).parents[1] / "interfaces/String.json")
    durability = (
        DurabilityPolicy.TRANSIENT_LOCAL if args.transient_local
        else DurabilityPolicy.VOLATILE
    )
    with Context(config=args.config) as context, Node(
        "listener", context=context
    ) as node:
        subscription = node.create_subscription(
            interface, args.topic,
            qos_profile=QoSProfile(depth=1, durability=durability),
        )
        print(json.dumps({"ready": True}), flush=True)
        for _ in range(args.count):
            print(json.dumps(subscription.recv(args.timeout)), flush=True)


if __name__ == "__main__":
    main()
