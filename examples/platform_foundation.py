"""Run a deployed service, Timer, Rate and ordinary parameter service.

Example Node config:
  {"name":"worker_east","namespace":"/east",
   "remaps":[{"kind":"service","from":"~/add","to":"add"}]}

Run from an installed SDK environment:
  python examples/platform_foundation.py --node-config deployment.json
"""

import argparse
import json
from pathlib import Path
import threading

import aos_sdk as aos


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, help="Zenoh session configuration")
    parser.add_argument("--node-config", type=Path, action="append", default=[])
    parser.add_argument("--node-name", help="Highest-priority deployment Node name")
    parser.add_argument("--namespace", help="Highest-priority deployment namespace")
    args = parser.parse_args()
    overrides = {
        key: value for key, value in
        (("name", args.node_name), ("namespace", args.namespace))
        if value is not None
    }
    interface = aos.Interface.load(
        Path(__file__).resolve().parents[1] / "interfaces/AddTwoInts.json"
    )
    with aos.Context(config=args.config) as context:
        with aos.Node(
            "worker", "/demo", context,
            config_files=args.node_config, cli_overrides=overrides,
        ) as node:
            node.declare_parameter(
                "label", "ready",
                aos.ParameterDescriptor(description="Label exposed by this deployment"),
            )
            node.add_on_set_parameters_callback(
                lambda candidate: aos.SetParametersResult(
                    all(value.name != "label" or value.value.strip() for value in candidate),
                    "label must not be blank",
                )
            )
            node.create_service(
                interface, "~/add", lambda request: {"sum": request["a"] + request["b"]},
                qos_profile={"preset": "services_default", "depth": 2},
            )
            client = node.create_client(interface, "~/add", qos_profile=3)
            parameters = node.create_client(
                aos.parameter_service_interface("get_parameters"), "~/get_parameters",
                qos_profile="parameters",
            )
            steady = aos.Clock(aos.ClockType.STEADY, context=context)
            timer_ready = threading.Event()
            timer_info = []

            def tick(info):
                timer.cancel()
                timer_info.append({
                    "expected_call_time": info.expected_call_time,
                    "actual_call_time": info.actual_call_time,
                })
                timer_ready.set()

            # This demonstration still progresses when deployment config selects
            # an uninitialized/paused ROS clock for Node business time.
            timer = node.create_timer(0.01, tick, clock=steady)
            rate = node.create_rate(200, clock=steady)
            executor = aos.MultiThreadedExecutor(2, context=context)
            executor.add_node(node)
            spinner = threading.Thread(target=executor.spin, name="platform-example")
            spinner.start()
            try:
                if not client.wait_for_service(timeout_sec=5):
                    raise RuntimeError("deployed service was not discovered within 5 seconds")
                if not parameters.wait_for_service(timeout_sec=5):
                    raise RuntimeError("parameter service was not discovered within 5 seconds")
                result = client.call_async({"a": 17, "b": 25}, timeout=5).result(6)
                parameter_result = parameters.call_async({"names": ["label"]}, timeout=5).result(6)
                rate.sleep()
                if not timer_ready.wait(2):
                    raise RuntimeError("steady Timer callback did not run")
                now = node.clock.now()
                print(json.dumps({
                    "node": node.fully_qualified_name,
                    "effective_config": node.effective_config,
                    "service": node.resolve_name_details("~/add", kind="service"),
                    "result": result,
                    "parameters": parameter_result,
                    "clock": {
                        "type": now.clock_type.value, "initialized": now.initialized,
                        "nanoseconds": now.nanoseconds, "use_sim_time": now.use_sim_time,
                    },
                    "timer": timer_info[0],
                }, ensure_ascii=False))
            finally:
                executor.shutdown(timeout_sec=5)
                spinner.join(5)
                if spinner.is_alive():
                    raise RuntimeError("executor did not stop")


if __name__ == "__main__":
    main()
