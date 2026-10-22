"""Business transactions and actual file exports; no AOS or network fixtures."""

import os
import concurrent.futures
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
import uuid

EXAMPLE = Path(__file__).resolve().parents[2] / "examples/memory_server"
sys.path.insert(0, str(EXAMPLE))
from memory_store import BusinessError, Limits, MemoryStore, Publication


class MemoryStoreTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="aos-memory-business-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.work = self.directory / "data"
        self.store = MemoryStore(self.work)
        self.addCleanup(lambda: self.store.close())

    def put(self, key, text="synthetic note", **kwargs):
        request_id = kwargs.pop("request_id", uuid.uuid4().hex)
        return self.store.put({"request_id": request_id, "key": key, "text": text, **kwargs})

    def export(self, **kwargs):
        publication = kwargs.pop("publication", Publication())
        return self.store.export(kwargs.pop("goal_id", uuid.uuid4().hex),
                                 kwargs.pop("request", {}), kwargs.pop("limits", Limits()),
                                 publication, feedback=kwargs.pop("feedback", lambda _: None), **kwargs)

    def test_transactions_replay_exact_result_after_update_and_restart(self):
        first = self.put("note", request_id="create")
        self.assertTrue(first["ok"])
        second = self.put("note", "new synthetic note", request_id="update", expected_revision=1)
        self.assertEqual(second["data"]["revision"], 2)
        self.assertEqual(self.put("note", request_id="create"), first)
        self.assertEqual(self.put("note", "different", request_id="create")["code"], "REQUEST_ID_CONFLICT")
        old_instance = self.store.instance_id
        self.store.close()
        self.store = MemoryStore(self.work)
        self.assertNotEqual(self.store.instance_id, old_instance)
        self.assertEqual(self.put("note", request_id="create"), first)
        self.assertEqual(self.store.get({"key": "note"})["data"]["text"], "new synthetic note")
        self.assertEqual(self.store.stats(), {"commit_seq": 2, "records": 1,
                                            "total_bytes": len("new synthetic note"),
                                            "requests": 2, "active_exports": 0})

    def test_concurrent_duplicate_request_commits_once(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as workers:
            results = list(workers.map(lambda _: self.put("note", request_id="same-request"), range(32)))
        self.assertTrue(all(value == results[0] for value in results))
        self.assertEqual(self.store.stats()["commit_seq"], 1)
        self.assertEqual(self.store.stats()["requests"], 1)

    def test_ledger_and_note_roll_back_together_when_the_ledger_insert_fails(self):
        self.store._connection.execute("""CREATE TRIGGER reject_request BEFORE INSERT ON requests
            BEGIN SELECT RAISE(ABORT, 'injected ledger failure'); END""")
        with self.assertRaises(sqlite3.IntegrityError):
            self.put("note", request_id="atomic")
        self.assertEqual(self.store.get({"key": "note"})["code"], "NOT_FOUND")
        self.assertEqual(self.store.stats()["commit_seq"], 0)
        self.assertEqual(self.store.stats()["requests"], 0)
        self.store._connection.execute("DROP TRIGGER reject_request")
        self.assertTrue(self.put("note", request_id="atomic")["ok"])

    def test_revision_conflict_is_recorded_and_does_not_turn_into_a_later_write(self):
        failed = self.put("note", request_id="premature", expected_revision=1)
        self.assertEqual(failed["code"], "REVISION_CONFLICT")
        self.assertTrue(self.put("note", request_id="create")["ok"])
        self.assertEqual(self.put("note", request_id="premature", expected_revision=1), failed)
        self.assertEqual(self.store.stats()["commit_seq"], 1)

    def test_utf8_capacity_and_full_ledger_preserve_existing_replay(self):
        limits = replace(Limits(), max_record_bytes=3, max_total_bytes=3, max_requests=2)
        request = {"request_id": "a", "key": "one", "text": "中"}
        first = self.store.put(request, limits)
        self.assertEqual(first["data"]["text_bytes"], 3)
        failed = self.store.put({"request_id": "b", "key": "two", "text": "x"}, limits)
        self.assertEqual(failed["code"], "CAPACITY")
        self.assertEqual(self.store.put(request, limits), first)
        full = self.store.put({"request_id": "c", "key": "one", "text": "a", "expected_revision": 1}, limits)
        self.assertEqual(full["code"], "CAPACITY")
        self.assertEqual(self.store.stats()["requests"], 2)
        self.assertEqual(self.store.stats()["commit_seq"], 1)

    def test_stable_pagination_uses_literal_prefix_and_does_not_return_note_text(self):
        for key in ("n_a", "n_b", "n_c", "nXa"):
            self.assertTrue(self.put(key)["ok"])
        first = self.store.list({"prefix": "n_", "limit": 2})["data"]
        self.assertEqual([row["key"] for row in first["items"]], ["n_a", "n_b"])
        self.assertTrue(first["has_more"])
        second = self.store.list({"prefix": "n_", "after_key": first["next_after"], "limit": 2})["data"]
        self.assertEqual([row["key"] for row in second["items"]], ["n_c"])
        self.assertFalse(second["has_more"])
        self.assertNotIn("text", first["items"][0])

    def test_parameter_capacity_cannot_be_shrunk_below_committed_usage(self):
        self.put("note", "four")
        for name, value in (("max_record_bytes", 3), ("max_total_bytes", 3)):
            with self.subTest(name=name), self.assertRaises(BusinessError) as caught:
                self.store.validate_limits(replace(Limits(), **{name: value}))
            self.assertEqual(caught.exception.code, "CAPACITY")
        self.store.validate_limits(replace(Limits(), max_record_bytes=4, max_total_bytes=4))

    def test_export_is_real_ordered_jsonl_with_a_stable_read_snapshot(self):
        for key in ("c", "a", "b"):
            self.put(key, "before-" + key)
        progress = []

        def mutate_after_first_batch(value, publication):
            if value["records_written"] == 1:
                self.assertTrue(self.put("b", "after-b", expected_revision=1)["ok"])
                self.assertTrue(self.put("d", "after-d")["ok"])

        result = self.export(limits=replace(Limits(), export_batch_size=1), feedback=progress.append,
                             batch_gate=mutate_after_first_batch)
        content = (self.work / result["file"]).read_bytes()
        rows = [json.loads(line) for line in content.splitlines()]
        self.assertEqual([row["key"] for row in rows], ["a", "b", "c"])
        self.assertEqual([row["text"] for row in rows], ["before-a", "before-b", "before-c"])
        self.assertEqual(result["snapshot_seq"], 3)
        self.assertEqual(result["sha256"], hashlib.sha256(content).hexdigest())
        self.assertEqual(result["bytes"], len(content))
        self.assertEqual([value["records_written"] for value in progress], [1, 2, 3])
        self.assertEqual(self.store.stats()["commit_seq"], 5)

    def test_cancel_intent_wins_before_publication_even_without_native_canceling(self):
        self.put("note")
        publication = Publication()

        def before_commit(value):
            self.assertTrue(value.request_cancel())

        with self.assertRaises(BusinessError) as caught:
            self.export(publication=publication, before_publish=before_commit)
        self.assertEqual(caught.exception.code, "CANCELED")
        self.assertFalse(publication.published)
        self.assertEqual(list(self.store.exports.iterdir()), [])
        self.assertEqual(self.store.stats()["active_exports"], 0)

    def test_completed_file_rejects_cancel_and_is_not_overwritten_by_reused_goal_id(self):
        self.put("note")
        goal_id, publication = uuid.uuid4().hex, Publication()
        first = self.export(goal_id=goal_id, publication=publication)
        self.assertFalse(publication.request_cancel())
        self.assertTrue(publication.published)
        with self.assertRaises(BusinessError) as caught:
            self.export(goal_id=goal_id)
        self.assertEqual(caught.exception.code, "EXPORT_EXISTS")
        self.assertEqual(hashlib.sha256((self.work / first["file"]).read_bytes()).hexdigest(), first["sha256"])
        self.assertFalse(list(self.store.exports.glob(".pending-*")))

    def test_record_byte_and_io_failures_never_publish_partial_exports(self):
        self.put("one")
        self.put("two")
        cases = [
            ({"request": {"max_records": 1}}, "EXPORT_RECORD_LIMIT"),
            ({"limits": replace(Limits(), export_bytes_limit=1)}, "EXPORT_BYTE_LIMIT"),
        ]
        for kwargs, code in cases:
            with self.subTest(code=code), self.assertRaises(BusinessError) as caught:
                self.export(**kwargs)
            self.assertEqual(caught.exception.code, code)
            self.assertEqual(list(self.store.exports.iterdir()), [])

        def feedback_failure(_):
            raise OSError("injected feedback failure")

        with self.assertRaises(OSError):
            self.export(feedback=feedback_failure)
        self.assertEqual(list(self.store.exports.iterdir()), [])
        self.assertEqual(self.store.stats()["active_exports"], 0)

    def test_owned_directory_lock_restart_cleanup_and_foreign_files(self):
        with self.assertRaises(BusinessError):
            MemoryStore(self.work)
        pending = self.store.exports / (".pending-" + uuid.uuid4().hex + ".jsonl")
        pending.write_text("unfinished synthetic export")
        foreign = self.store.exports / "leave-me.txt"
        foreign.write_text("unrelated fixture")
        self.store.close()
        self.store = MemoryStore(self.work)
        self.assertFalse(pending.exists())
        self.assertEqual(foreign.read_text(), "unrelated fixture")
        unrelated = self.directory / "unowned"
        unrelated.mkdir()
        (unrelated / "important.txt").write_text("untouched")
        with self.assertRaises(BusinessError):
            MemoryStore(unrelated)
        self.assertEqual((unrelated / "important.txt").read_text(), "untouched")
        link = self.directory / "link"
        from process_support import directory_alias
        directory_alias(link, self.work)
        with self.assertRaises(BusinessError):
            MemoryStore(link)

    def test_audit_is_bounded_and_has_ids_without_note_body(self):
        self.store.close()
        self.store = MemoryStore(self.work, audit_bytes=1024)
        for index in range(50):
            self.put(f"note-{index}", "PRIVATE_SYNTHETIC_BODY", request_id=f"request-{index}")
        logs = list(self.work.glob("audit*.jsonl"))
        self.assertEqual(len(logs), 2)
        for path in logs:
            self.assertLessEqual(path.stat().st_size, 1024)
            self.assertNotIn("PRIVATE_SYNTHETIC_BODY", path.read_text())
        events = [json.loads(line) for path in logs for line in path.read_text().splitlines()]
        self.assertTrue(all(value["instance_id"] == self.store.instance_id for value in events))
        self.assertIn("request_id", events[-1])


if __name__ == "__main__":
    unittest.main()
