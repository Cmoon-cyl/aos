"""Persistent SQLite MemoryServer. Start inactive; configure and activate to serve.

The caller must select a work directory. No user notes are imported. AOS run and
launch supply the normal process identity/configuration; --autostart is useful
for an explicitly invoked standalone run. Installed launch plans use readiness
to configure and activate the provider instead.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import signal
import sqlite3
import sys
import threading
import time

import aos_sdk as aos

# -I excludes cwd and PYTHONPATH. Only this exact installed source directory is
# added for the example's sibling business module, never the work directory.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from memory_store import BusinessError, Limits, MemoryStore, Publication, response
from request_observer import exporter_from_args

CONTRACT_VERSION = "1.0.0"
PARAMETER_DESCRIPTIONS = {
    "max_record_bytes": "Maximum UTF-8 bytes in one note. Shrinking below an existing note is rejected.",
    "max_records": "Maximum distinct note keys. Existing keys may be updated within other budgets.",
    "max_total_bytes": "Maximum sum of committed UTF-8 note bytes; excludes SQLite/WAL and export file overhead.",
    "max_requests": "Maximum durable request_id outcomes. No automatic eviction; existing IDs remain replayable at capacity.",
    "export_batch_size": "Actual SQLite rows fetched/written per export batch; cancellation is checked between rows.",
    "export_bytes_limit": "Maximum bytes in each complete JSONL export file; exceeding it publishes no file.",
}


def load_interfaces(directory=None):
    if directory is None:
        return {name: aos.Interface.load(aos.get_package_resource("aos_memory", name.lower()))
                for name in ("Put", "Get", "List", "Status", "Export")}
    directory = Path(directory)
    return {name: aos.Interface.load(directory / f"{name}.json")
            for name in ("Put", "Get", "List", "Status", "Export")}


class MemoryNode(aos.LifecycleNode):
    """A normal SDK lifecycle node; Rust owns lifecycle and all Action states."""
    def __init__(self, store, *, context, name="server", namespace="/memory",
                 interface_directory=None, parameter_overrides=None,
                 batch_gate=None, before_publish=None, after_publish=None, cancel_decision_gate=None):
        self.store = store
        self._jobs = {}
        self._jobs_lock = threading.Lock()
        self._stopping = threading.Event()
        self._cancel_poll = threading.Event()
        self._sequence = 0
        self._batch_gate, self._before_publish = batch_gate, before_publish
        self._after_publish = after_publish
        self._cancel_decision_gate = cancel_decision_gate
        super().__init__(name, namespace, context, display_name="SQLite Memory Server",
            summary=(
                "Persistent synthetic-note example in a caller-selected work directory. "
                "Configure then activate before calling put/get/list or export. "
                "put atomically binds request_id to its arguments/result and uses revision compare-and-set; "
                "an RPC timeout does not prove a write was not committed. Only explicitly repeat the same request_id. "
                "get reads text; list pages metadata. status emits counters every second while active, even without consumers. "
                "export writes one bounded JSONL snapshot on the provider host, reports SHA-256 and supports cancel before publication. "
                "Change capacity parameters only while inactive. Restart preserves committed data/files, not old Action goal states."
            ), tags=("memory", "sqlite", "persistent", "lifecycle"),
            parameter_overrides=parameter_overrides)
        try:
            for key, (low, high) in Limits.RANGES.items():
                self.declare_parameter(key, getattr(Limits(), key), aos.ParameterDescriptor(
                    description=PARAMETER_DESCRIPTIONS[key], integer_range=aos.IntegerRange(low, high),
                    allowed_states=("inactive",),
                ))
            self.add_on_set_parameters_callback(self._validate_parameters)
            self.interfaces = load_interfaces(interface_directory)
            self.create_service(self.interfaces["Put"], "put", self._put,
                summary="Create/update one UTF-8 note. request_id and result commit atomically; expected_revision=0 creates only. Same ID with changed arguments is rejected.",
                side_effects="write", contract_version=CONTRACT_VERSION)
            self.create_service(self.interfaces["Get"], "get", self._get,
                summary="Read one committed note and its revision/hash. NOT_FOUND is a business response; no file or database mutation.",
                side_effects="read_only", contract_version=CONTRACT_VERSION)
            self.create_service(self.interfaces["List"], "list", self._list,
                summary="Read up to 200 note metadata entries after an exclusive key cursor. Literal prefix, sorted keys, no note text; separate pages need not share a snapshot.",
                side_effects="read_only", contract_version=CONTRACT_VERSION)
            self._status = self.create_publisher(self.interfaces["Status"], "status",
                summary="One counter snapshot per second while active, including without consumers. No note text; no inactive backlog replay.",
                side_effects="read_only", contract_version=CONTRACT_VERSION)
            self.create_timer(1.0, self._publish_status)
            self.action = aos.ActionServer(self, self.interfaces["Export"], "export", self._export,
                goal_callback=self._accept_export, handle_accepted_callback=self._accepted_export,
                cancel_callback=self._cancel_export, result_timeout=60.0, max_result_waiters=32,
                resource_limits={"max_goals": 128, "max_goal_bytes": 4096,
                                 "max_result_bytes": 4096, "max_result_cache_bytes": 512 * 1024},
                summary="Write a consistent SQLite snapshot as bounded JSONL under the provider's work directory. Feedback counts real written rows/bytes. Complete-file publication and cancellation share a business lock; cancel after publication is rejected. Each goal creates an independent file; old goals are not restored after restart.",
                side_effects="write", contract_version=CONTRACT_VERSION)
        except BaseException:
            self.destroy_node()
            raise

    def limits(self):
        return Limits.from_mapping({parameter.name: parameter.value
                                    for parameter in self.get_parameters(Limits.RANGES)})

    def _validate_parameters(self, parameters):
        try:
            values = {parameter.name: parameter.value for parameter in self.get_parameters(Limits.RANGES)}
            values.update({parameter.name: parameter.value for parameter in parameters
                           if parameter.name in Limits.RANGES})
            self.store.validate_limits(Limits.from_mapping(values))
            return aos.SetParametersResult(True)
        except BusinessError as error:
            return aos.SetParametersResult(False, error.message)

    def on_configure(self, state):
        try:
            self.store.validate_limits(self.limits())
            return aos.TransitionCallbackReturn.SUCCESS
        except BusinessError as error:
            self.store.audit.write("configure_rejected", code=error.code)
            return aos.TransitionCallbackReturn.FAILURE

    def on_shutdown(self, state):
        self.request_stop()
        return aos.TransitionCallbackReturn.SUCCESS

    def request_stop(self):
        self._stopping.set()

    def _call(self, operation, request, *arguments):
        try:
            return operation(request, *arguments)
        except BusinessError as error:
            return error.response()
        except (OSError, sqlite3.Error):
            # A failed/late response never authorizes automatic replay. The
            # durable request_id ledger remains the authority for put outcomes.
            self.store.audit.write("storage_error", code="STORAGE_ERROR")
            return response("STORAGE_ERROR", "Storage operation failed; a put outcome may require explicit same-ID reconciliation.")

    def _put(self, request):
        return self._call(self.store.put, request, self.limits())

    def _get(self, request):
        return self._call(self.store.get, request)

    def _list(self, request):
        return self._call(self.store.list, request)

    def _publish_status(self):
        self._sequence += 1
        value = self.store.stats() | {"instance_id": self.store.instance_id,
            "sequence": self._sequence, "lifecycle": self.get_current_state().label,
            "audit_errors": self.store.audit.errors}
        try:
            self._status.publish(value)
            self.store.audit.write("status_published", commit_seq=value["commit_seq"],
                                   records=value["records"], bytes=value["total_bytes"])
            return True
        except aos.InactiveError:
            # Native lifecycle admission may close after this timer was taken.
            return False

    def _accept_export(self, request):
        with self._jobs_lock:
            return (aos.GoalResponse.ACCEPT if not self._stopping.is_set() and not self._jobs
                    else aos.GoalResponse.REJECT)

    def _accepted_export(self, goal):
        with self._jobs_lock:
            self._jobs[goal.goal_id] = Publication()
        self.store.audit.write("export_admitted", goal_id=goal.goal_id)
        try:
            goal.execute()
        except BaseException:
            with self._jobs_lock:
                self._jobs.pop(goal.goal_id, None)
            raise

    def _cancel_export(self, goal):
        with self._jobs_lock:
            publication = self._jobs.get(goal.goal_id)
        if publication is None or not publication.request_cancel():
            return aos.CancelResponse.REJECT
        self.store.audit.write("export_cancel_intent", goal_id=goal.goal_id)
        if self._cancel_decision_gate is not None:
            self._cancel_decision_gate(goal, publication)
        return aos.CancelResponse.ACCEPT

    def _finish_canceled(self, goal):
        # cancel_callback marks the business intent before the SDK applies its
        # native state transition. Wait only for that real control decision;
        # this bounded wait is not simulated export work.
        deadline = time.monotonic() + 2.0
        while not goal.is_cancel_requested and goal.is_active and time.monotonic() < deadline:
            self._cancel_poll.wait(min(0.005, max(0.0, deadline - time.monotonic())))
        if goal.is_cancel_requested:
            goal.canceled()
            return response("CANCELED", "Export canceled before file publication.")
        goal.abort()
        return response("CANCEL_CONFIRMATION_TIMEOUT", "No native cancellation transition was observed; no file was published.")

    def _export(self, goal):
        with self._jobs_lock:
            publication = self._jobs[goal.goal_id]
        result = None
        try:
            value = self.store.export(goal.goal_id, goal.request, self.limits(), publication,
                feedback=goal.publish_feedback, stop=self._stopping,
                batch_gate=self._batch_gate, before_publish=self._before_publish,
                after_publish=self._after_publish)
            goal.succeed()
            result = response(data=value)
        except BusinessError as error:
            if publication.cancel_intent.is_set() and not publication.published:
                result = self._finish_canceled(goal)
            else:
                goal.abort()
                result = error.response()
        except (OSError, sqlite3.Error):
            if publication.cancel_intent.is_set() and not publication.published:
                result = self._finish_canceled(goal)
            else:
                goal.abort()
                result = response("STORAGE_ERROR", "Export I/O failed; inspect the owned directory and bounded audit log.")
        except Exception:
            if publication.cancel_intent.is_set() and not publication.published:
                result = self._finish_canceled(goal)
            else:
                goal.abort()
                result = response("EXPORT_ERROR", "Export failed before a normal completion result; inspect the bounded audit log.")
        finally:
            with self._jobs_lock:
                self._jobs.pop(goal.goal_id, None)
            self.store.audit.write("export_finished", goal_id=goal.goal_id, status=int(goal.status),
                                   code=result["code"] if result is not None else "EXPORT_ERROR")
        return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True,
                        help="Empty or previously owned directory for this example's database, exports and bounded audit.")
    selected = parser.add_mutually_exclusive_group()
    selected.add_argument("--config", type=Path, help="Explicit plaintext session config; otherwise inherit the AOS process configuration.")
    selected.add_argument("--security-profile", type=Path, help="Explicit absolute client security profile; never copied into the data directory.")
    parser.add_argument("--domain-id", type=int)
    parser.add_argument("--name", default="server")
    parser.add_argument("--namespace", default="/memory")
    parser.add_argument("--interfaces", type=Path, help="Explicit interface directory for source development; installed entries use the package index.")
    parser.add_argument("--telemetry-topic", help="Opt in to real Core request metadata on this absolute Topic; exclusive capture/drain ownership.")
    parser.add_argument("--autostart", action="store_true", help="Configure/activate locally after starting the executor; installed launch readiness normally does this.")
    args = parser.parse_args(argv)
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, lambda *_: stop.set())
    options = {}
    for key in ("config", "security_profile", "domain_id"):
        value = getattr(args, key)
        if value is not None:
            options[key] = value
    with MemoryStore(args.work_dir) as store, aos.Context(**options) as context:
        executor = aos.MultiThreadedExecutor(4, context=context, flush_logs=False)
        with exporter_from_args(args, context, options, known_executors=(executor,)):
            with MemoryNode(store, context=context, name=args.name, namespace=args.namespace,
                            interface_directory=args.interfaces) as node:
                executor.add_node(node)
                failures = []

                def spin():
                    try:
                        executor.spin()
                    except BaseException as error:
                        failures.append(error)
                        stop.set()

                spinner = threading.Thread(target=spin, name="memory-server-executor")
                spinner.start()
                try:
                    if args.autostart:
                        for transition in (node.trigger_configure_async, node.trigger_activate_async):
                            report = transition().result(5)
                            if not report["successful"]:
                                raise RuntimeError("MemoryServer lifecycle autostart failed")
                    print(json.dumps({"event": "memory_server_initialized", "node": node.fully_qualified_name,
                        "instance_id": store.instance_id, "lifecycle": node.get_current_state().label,
                        "work_dir": str(store.directory)}, ensure_ascii=False), flush=True)
                    stop.wait()
                finally:
                    node.request_stop()
                    cleanup_error = None
                    try:
                        if context.ok() and not failures and node.get_current_state().label != "finalized":
                            report = node.trigger_shutdown_async(drain_timeout=5).result(6)
                            if not report["successful"]:
                                cleanup_error = RuntimeError("MemoryServer lifecycle shutdown did not succeed")
                    except Exception as error:
                        cleanup_error = error
                    finally:
                        stopped = executor.shutdown(timeout_sec=6)
                        spinner.join(6)
                    print(json.dumps({"event": "memory_server_stopped", "instance_id": store.instance_id,
                                      "executor_stopped": stopped and not spinner.is_alive(),
                                      "commit_seq": store.stats()["commit_seq"]}), flush=True)
                    if not stopped or spinner.is_alive():
                        raise RuntimeError("MemoryServer executor did not stop")
                    if cleanup_error is not None:
                        raise RuntimeError("MemoryServer lifecycle cleanup failed") from cleanup_error
                if failures:
                    raise RuntimeError("MemoryServer executor failed") from failures[0]


if __name__ == "__main__":
    main()
