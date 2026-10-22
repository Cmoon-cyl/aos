"""Run with: python -m examples.typed_talker --config <session.json5>."""

import argparse
import json
from pathlib import Path
import time

from aos_sdk import Context, DurabilityPolicy, Interface, Node, QoSProfile


def main():
    parser = argparse.ArgumentParser(description="Publish typed JSON messages.")
    parser.add_argument("--config")
    parser.add_argument("--topic", default="/chatter")
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--interval", type=float, default=0.2)
    parser.add_argument("--transient-local", action="store_true")
    parser.add_argument("--hold", type=float, default=0)
    args = parser.parse_args()
    interface = Interface.load(Path(__file__).parents[1] / "interfaces/String.json")
    durability = (
        DurabilityPolicy.TRANSIENT_LOCAL if args.transient_local
        else DurabilityPolicy.VOLATILE
    )
    with Context(config=args.config) as context, Node(
        "talker", context=context, summary="Publishes a text counter."
    ) as node:
        publisher = node.create_publisher(
            interface, args.topic, QoSProfile(depth=1, durability=durability),
            summary="Counter text", side_effects="read_only", contract_version="1",
        )
        # Let already-running volatile subscribers discover this publisher.
        time.sleep(0.3)
        for count in range(args.count):
            message = {"data": f"hello AOS {count}"}
            publisher.publish(message)
            print(json.dumps(message), flush=True)
            time.sleep(args.interval)
        time.sleep(args.hold)


if __name__ == "__main__":
    main()
