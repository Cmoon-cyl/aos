"""Run with: python -m examples.add_two_ints_server --config <session.json5>."""

import argparse
import json
from pathlib import Path
import sys
import threading

from aos_sdk import Context, Interface, Node, SingleThreadedExecutor


def main():
    parser = argparse.ArgumentParser(description="Serve AddTwoInts requests.")
    parser.add_argument("--config")
    parser.add_argument("--service", default="/add_two_ints")
    parser.add_argument("--namespace", default="/")
    parser.add_argument("--stop-on-stdin", action="store_true",
                        help="stop normally when stdin receives 'stop'")
    args = parser.parse_args()
    interface = Interface.load(Path(__file__).parents[1] / "interfaces/AddTwoInts.json")
    with Context(config=args.config) as context, Node(
        "calculator", args.namespace, context,
        display_name="Integer calculator", summary="Adds two integers.", tags=["math"],
    ) as node, SingleThreadedExecutor(context=context) as executor:
        node.create_service(
            interface, args.service, lambda request: {"sum": request["a"] + request["b"]},
            summary="Add two integers without changing state.",
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
        print(json.dumps({"ready": True, "node": node.fully_qualified_name}), flush=True)
        try:
            if args.stop_on_stdin:
                while context.ok() and not stop.is_set():
                    executor.spin_once(timeout_sec=0.1)
            else:
                executor.spin()
        except KeyboardInterrupt:
            pass
        finally:
            if context.ok():
                context.shutdown()


if __name__ == "__main__":
    main()
