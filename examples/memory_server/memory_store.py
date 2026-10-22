"""Bounded SQLite note storage and complete-file export, independent of AOS.

The database is business data. It never stores or restores Action goal states.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading
import time
import uuid


FORMAT = "aos-memory-v1"
APPLICATION_ID = 0x414F534D
IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_./:-]{0,127}\Z", re.ASCII)
PENDING = re.compile(r"\.pending-[0-9a-f]{32}\.jsonl\Z", re.ASCII)


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                      allow_nan=False)


def response(code="OK", message="", data=None):
    return {"ok": code == "OK", "code": code, "message": message, "data": data}


class BusinessError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code, self.message = code, message

    def response(self):
        return response(self.code, self.message)


@dataclass(frozen=True)
class Limits:
    max_record_bytes: int = 16 * 1024
    max_records: int = 10_000
    max_total_bytes: int = 64 * 1024 * 1024
    max_requests: int = 100_000
    export_batch_size: int = 64
    export_bytes_limit: int = 64 * 1024 * 1024

    RANGES = {
        "max_record_bytes": (1, 65_536), "max_records": (1, 100_000),
        "max_total_bytes": (1, 256 * 1024 * 1024), "max_requests": (1, 1_000_000),
        "export_batch_size": (1, 512), "export_bytes_limit": (1, 64 * 1024 * 1024),
    }

    @classmethod
    def from_mapping(cls, values):
        if set(values) - cls.RANGES.keys():
            raise BusinessError("INVALID", "Unknown capacity parameter.")
        limits = replace(cls(), **values)
        for name, (low, high) in cls.RANGES.items():
            value = getattr(limits, name)
            if type(value) is not int or not low <= value <= high:
                raise BusinessError("INVALID", f"{name} must be an integer in {low}..{high}.")
        return limits


@dataclass
class Publication:
    """Ephemeral file-publication decision; not an Action state machine."""
    lock: threading.Lock = field(default_factory=threading.Lock)
    cancel_intent: threading.Event = field(default_factory=threading.Event)
    closed_event: threading.Event = field(default_factory=threading.Event)
    published: bool = False
    closed: bool = False

    def request_cancel(self):
        with self.lock:
            if self.published or self.closed:
                return False
            # The native CANCELING edge happens after cancel_callback returns.
            # File publication must already observe this business intent.
            self.cancel_intent.set()
            return True

    def check(self, stop=None):
        if self.cancel_intent.is_set():
            raise BusinessError("CANCELED", "Export canceled before file publication.")
        if stop is not None and stop.is_set():
            raise BusinessError("SHUTDOWN", "Export stopped before file publication.")


class AuditLog:
    """Two bounded JSONL files; failures are visible without changing commits."""
    def __init__(self, directory, instance_id, max_bytes=256 * 1024):
        self.directory, self.instance_id = directory, instance_id
        self.max_bytes = max_bytes
        self.errors = 0
        self._lock = threading.Lock()

    def write(self, event, **fields):
        allowed = {"request_id", "commit_seq", "goal_id", "code", "status", "records", "bytes"}
        if set(fields) - allowed:
            raise ValueError("Audit fields must not include note content or arbitrary data")
        value = {"event": event, "instance_id": self.instance_id,
                 "time_unix_ns": time.time_ns(), **fields}
        encoded = (canonical(value) + "\n").encode("utf-8")
        if len(encoded) > self.max_bytes:
            raise ValueError("Audit event exceeds the file budget")
        path = self.directory / "audit.jsonl"
        previous = self.directory / "audit.previous.jsonl"
        with self._lock:
            try:
                for selected in (path, previous):
                    if selected.exists() or path_alias(selected):
                        if not stat.S_ISREG(selected.lstat().st_mode):
                            raise OSError("Audit path is not a regular file")
                if path.exists() and path.stat().st_size + len(encoded) > self.max_bytes:
                    os.replace(path, previous)
                descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND |
                                     getattr(os, "O_NOFOLLOW", 0), 0o600)
                with os.fdopen(descriptor, "ab") as output:
                    output.write(encoded)
            except OSError:
                self.errors += 1


def path_alias(path):
    # Junctions are reparse points but Path.is_symlink() is false on Python 3.11.
    return path.is_symlink() or (os.name == "nt" and path.exists() and
        bool(path.lstat().st_file_attributes & 0x400))


class MemoryStore:
    """One process owns a caller-selected data directory and its SQLite writer."""
    def __init__(self, work_dir, *, instance_id=None, audit_bytes=256 * 1024):

        self.directory = Path(work_dir).absolute()
        if path_alias(self.directory):
            raise BusinessError("INVALID", "The work directory must not be a symlink.")
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        marker = self.directory / ".aos-memory.json"
        existing = marker.exists()
        if not existing and any(self.directory.iterdir()):
            raise BusinessError("INVALID", "Use an empty directory or an existing AOS memory directory.")
        if existing:
            if path_alias(marker) or not marker.is_file() or marker.stat().st_size > 4096:
                raise BusinessError("INVALID", "Invalid memory directory marker.")
            try:
                owner = json.loads(marker.read_text(encoding="utf-8"))
                if owner["format"] != FORMAT:
                    raise ValueError("format")
                self.store_id = uuid.UUID(owner["store_id"]).hex
            except (KeyError, ValueError, TypeError, OSError) as error:
                raise BusinessError("INVALID", "Unsupported memory directory marker.") from error
        else:
            self.store_id = uuid.uuid4().hex
        self.instance_id = instance_id or uuid.uuid4().hex
        self._lock = threading.RLock()
        self._active_exports = 0
        self._closed = False
        self._file_lock = None
        self._connection = None
        self.path = self.directory / "memory.sqlite3"
        self.exports = self.directory / "exports"
        try:
            if path_alias(self.directory / ".aos-memory.lock"):
                raise BusinessError("INVALID", "The lock path must not be an alias.")
            self._file_lock = os.open(self.directory / ".aos-memory.lock",
                                      os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
            try:
                if os.name == "nt":
                    import msvcrt
                    # Lock byte zero, even for a new empty lock file. Windows
                    # releases byte-range locks when this descriptor closes.
                    os.lseek(self._file_lock, 0, os.SEEK_SET)
                    msvcrt.locking(self._file_lock, msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(self._file_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as error:
                raise BusinessError("INVALID", "This memory directory is already in use.") from error
            if existing and not self.path.is_file():
                raise BusinessError("INVALID", "The owned database is missing; explicit recovery is required.")
            for name in ("memory.sqlite3", "memory.sqlite3-wal", "memory.sqlite3-shm"):
                selected = self.directory / name
                if path_alias(selected) or (selected.exists() and not selected.is_file()):
                    raise BusinessError("INVALID", "Database paths must be regular files.")
            if path_alias(self.exports):
                raise BusinessError("INVALID", "The export directory must not be a symlink.")
            self.exports.mkdir(mode=0o700, exist_ok=True)
            self._connection = sqlite3.connect(self.path, isolation_level=None, timeout=1,
                                               check_same_thread=False)
            os.chmod(self.path, 0o600)
            self._connection.row_factory = sqlite3.Row
            if existing:
                if self._connection.execute("PRAGMA application_id").fetchone()[0] != APPLICATION_ID:
                    raise BusinessError("INVALID", "Unsupported database application ID.")
                row = self._connection.execute("SELECT store_id, schema_version FROM metadata WHERE id=1").fetchone()
                if row is None or row[0] != self.store_id or row[1] != 1:
                    raise BusinessError("INVALID", "Database identity or schema does not match its directory.")
            self._connection.execute("PRAGMA journal_mode=WAL")
            self._connection.execute("PRAGMA synchronous=FULL")
            if not existing:
                self._connection.executescript(f"""
                    PRAGMA application_id={APPLICATION_ID};
                    CREATE TABLE metadata (
                        id INTEGER PRIMARY KEY CHECK(id=1), store_id TEXT NOT NULL,
                        schema_version INTEGER NOT NULL, commit_seq INTEGER NOT NULL,
                        records INTEGER NOT NULL, total_bytes INTEGER NOT NULL,
                        requests INTEGER NOT NULL);
                    CREATE TABLE notes (
                        key TEXT PRIMARY KEY, text TEXT NOT NULL, revision INTEGER NOT NULL,
                        sha256 TEXT NOT NULL, text_bytes INTEGER NOT NULL,
                        commit_seq INTEGER NOT NULL, updated_unix_ns INTEGER NOT NULL);
                    CREATE TABLE requests (
                        request_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, result_json TEXT NOT NULL);
                """)
                self._connection.execute("INSERT INTO metadata VALUES (1,?,1,0,0,0,0)", (self.store_id,))
                with marker.open("x", encoding="utf-8") as output:
                    output.write(canonical({"format": FORMAT, "store_id": self.store_id}) + "\n")
                    output.flush()
                    os.fsync(output.fileno())
            # Only this format's unfinished export names are eligible for cleanup.
            for pending in self.exports.iterdir():
                if PENDING.fullmatch(pending.name):
                    if path_alias(pending) or not pending.is_file():
                        raise BusinessError("INVALID", "An export temporary path is not a regular file.")
                    pending.unlink()
            self.audit = AuditLog(self.directory, self.instance_id, audit_bytes)
            self.audit.write("instance_opened", commit_seq=self.stats()["commit_seq"])
        except BaseException:
            self.close()
            raise

    def _open(self):
        if self._closed:
            raise BusinessError("STORAGE_ERROR", "The memory store is closed.")

    @staticmethod
    def _identifier(value, name, *, empty=False):
        if empty and value == "":
            return value
        if not isinstance(value, str) or IDENTIFIER.fullmatch(value) is None:
            raise BusinessError("INVALID", f"{name} must be a 1..128 character ASCII identifier.")
        return value

    def stats(self):
        with self._lock:
            self._open()
            row = self._connection.execute(
                "SELECT commit_seq, records, total_bytes, requests FROM metadata WHERE id=1"
            ).fetchone()
            return dict(row) | {"active_exports": self._active_exports}

    def validate_limits(self, limits):
        with self._lock:
            current = self.stats()
            largest = self._connection.execute("SELECT COALESCE(MAX(text_bytes),0) FROM notes").fetchone()[0]
            for name, actual in (("max_record_bytes", largest), ("max_records", current["records"]),
                                 ("max_total_bytes", current["total_bytes"]),
                                 ("max_requests", current["requests"])):
                if getattr(limits, name) < actual:
                    raise BusinessError("CAPACITY", f"{name} is below already committed usage.")

    def put(self, request, limits=Limits()):
        try:
            if not isinstance(request, dict) or set(request) - {"request_id", "key", "text", "expected_revision"}:
                raise BusinessError("INVALID", "put requires request_id, key, text and optional expected_revision.")
            request_id = self._identifier(request.get("request_id"), "request_id")
            key = self._identifier(request.get("key"), "key")
            text = request.get("text")
            revision = request.get("expected_revision", 0)
            if not isinstance(text, str) or not text or len(text) > 65_536:
                raise BusinessError("INVALID", "text must contain 1..65536 characters, within the UTF-8 byte budget.")
            if type(revision) is not int or not 0 <= revision <= 2**53 - 1:
                raise BusinessError("INVALID", "expected_revision must be an integer in 0..9007199254740991.")
            encoded = text.encode("utf-8")
            fingerprint = hashlib.sha256(canonical({
                "operation": "put", "key": key, "text": text, "expected_revision": revision,
            }).encode("utf-8")).hexdigest()
        except BusinessError as error:
            return error.response()
        except UnicodeError:
            return response("INVALID", "text must encode as UTF-8.")
        replayed = False
        with self._lock:
            self._open()
            connection = self._connection
            connection.execute("BEGIN IMMEDIATE")
            try:
                previous = connection.execute("SELECT fingerprint,result_json FROM requests WHERE request_id=?",
                                              (request_id,)).fetchone()
                if previous is not None:
                    replayed = True
                    result = (json.loads(previous["result_json"]) if previous["fingerprint"] == fingerprint
                              else response("REQUEST_ID_CONFLICT", "request_id is already bound to different arguments."))
                else:
                    stats = self.stats()
                    current = connection.execute("SELECT revision,text_bytes FROM notes WHERE key=?", (key,)).fetchone()
                    if stats["requests"] >= limits.max_requests:
                        result = response("CAPACITY", "The request ledger is full; existing IDs remain replayable.")
                    else:
                        old_revision = 0 if current is None else current["revision"]
                        size_delta = len(encoded) - (0 if current is None else current["text_bytes"])
                        if old_revision != revision:
                            result = response("REVISION_CONFLICT", "Read the current revision and use a new request_id.")
                        elif (len(encoded) > limits.max_record_bytes or
                              stats["total_bytes"] + size_delta > limits.max_total_bytes or
                              (current is None and stats["records"] >= limits.max_records)):
                            result = response("CAPACITY", "The requested record exceeds the configured capacity.")
                        else:
                            commit_seq = stats["commit_seq"] + 1
                            checksum = hashlib.sha256(encoded).hexdigest()
                            connection.execute("""INSERT INTO notes VALUES (?,?,?,?,?,?,?)
                                ON CONFLICT(key) DO UPDATE SET text=excluded.text, revision=excluded.revision,
                                sha256=excluded.sha256, text_bytes=excluded.text_bytes,
                                commit_seq=excluded.commit_seq, updated_unix_ns=excluded.updated_unix_ns""",
                                (key, text, revision + 1, checksum, len(encoded), commit_seq, time.time_ns()))
                            connection.execute("UPDATE metadata SET commit_seq=?,records=records+?,total_bytes=total_bytes+? WHERE id=1",
                                               (commit_seq, int(current is None), size_delta))
                            result = response(data={"request_id": request_id, "key": key, "revision": revision + 1,
                                "commit_seq": commit_seq, "sha256": checksum, "text_bytes": len(encoded),
                                "created": current is None})
                        connection.execute("INSERT INTO requests VALUES (?,?,?)", (request_id, fingerprint, canonical(result)))
                        connection.execute("UPDATE metadata SET requests=requests+1 WHERE id=1")
                connection.execute("COMMIT")
            except BaseException:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise
            sequence = self.stats()["commit_seq"]
        self.audit.write("request_replayed" if replayed else "put_finished", request_id=request_id,
                         commit_seq=(result["data"]["commit_seq"] if result["ok"] else sequence),
                         code=result["code"])
        return result

    def get(self, request):
        try:
            if not isinstance(request, dict) or set(request) != {"key"}:
                raise BusinessError("INVALID", "get requires only key.")
            key = self._identifier(request["key"], "key")
            with self._lock:
                self._open()
                row = self._connection.execute("SELECT * FROM notes WHERE key=?", (key,)).fetchone()
            return response(data=dict(row)) if row is not None else response("NOT_FOUND", "No record has this key.")
        except BusinessError as error:
            return error.response()

    def list(self, request):
        try:
            if not isinstance(request, dict) or set(request) - {"prefix", "after_key", "limit"}:
                raise BusinessError("INVALID", "list accepts prefix, after_key and limit.")
            prefix = self._identifier(request.get("prefix", ""), "prefix", empty=True)
            after = self._identifier(request.get("after_key", ""), "after_key", empty=True)
            limit = request.get("limit", 50)
            if type(limit) is not int or not 1 <= limit <= 200:
                raise BusinessError("INVALID", "limit must be an integer in 1..200.")
            with self._lock:
                self._open()
                rows = self._connection.execute("""SELECT key,revision,sha256,text_bytes,commit_seq,updated_unix_ns
                    FROM notes WHERE substr(key,1,length(?))=? AND key>? ORDER BY key LIMIT ?""",
                    (prefix, prefix, after, limit + 1)).fetchall()
                snapshot = self.stats()["commit_seq"]
            more = len(rows) > limit
            items = [dict(row) for row in rows[:limit]]
            return response(data={"items": items, "has_more": more,
                                  "next_after": items[-1]["key"] if more else "", "snapshot_seq": snapshot})
        except BusinessError as error:
            return error.response()

    def export(self, goal_id, request, limits, publication, *, feedback, stop=None,
               batch_gate=None, before_publish=None, after_publish=None):
        """Export one SQLite read snapshot, publishing only a complete bounded file.

        Batch/publication hooks are injected test fixtures, never CLI options.
        They do not generate synthetic work or progress.
        """
        if not isinstance(request, dict) or set(request) - {"prefix", "max_records"}:
            raise BusinessError("INVALID", "export accepts prefix and max_records.")
        prefix = self._identifier(request.get("prefix", ""), "prefix", empty=True)
        maximum = request.get("max_records", 10_000)
        if type(maximum) is not int or not 1 <= maximum <= 100_000:
            raise BusinessError("INVALID", "max_records must be an integer in 1..100000.")
        name = uuid.UUID(goal_id).hex
        temporary = self.exports / f".pending-{name}.jsonl"
        final = self.exports / f"{name}.jsonl"
        with self._lock:
            self._open()
            self._active_exports += 1
        connection = None
        try:
            publication.check(stop)
            connection = sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True, isolation_level=None, timeout=1)
            connection.row_factory = sqlite3.Row
            connection.execute("BEGIN")
            snapshot = connection.execute("SELECT commit_seq FROM metadata WHERE id=1").fetchone()[0]
            total = connection.execute("SELECT COUNT(*) FROM notes WHERE substr(key,1,length(?))=?",
                                       (prefix, prefix)).fetchone()[0]
            if total > maximum:
                raise BusinessError("EXPORT_RECORD_LIMIT", "The snapshot exceeds max_records; narrow the prefix or raise the bound.")
            digest, size, written = hashlib.sha256(), 0, 0
            cursor = connection.execute("SELECT * FROM notes WHERE substr(key,1,length(?))=? ORDER BY key",
                                        (prefix, prefix))
            descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                                 getattr(os, "O_NOFOLLOW", 0), 0o600)
            with os.fdopen(descriptor, "wb") as output:
                while True:
                    publication.check(stop)
                    batch = cursor.fetchmany(limits.export_batch_size)
                    if not batch:
                        break
                    for row in batch:
                        publication.check(stop)
                        encoded = (canonical(dict(row)) + "\n").encode("utf-8")
                        if size + len(encoded) > limits.export_bytes_limit:
                            raise BusinessError("EXPORT_BYTE_LIMIT", "The JSONL file exceeds export_bytes_limit.")
                        output.write(encoded)
                        digest.update(encoded)
                        size += len(encoded)
                        written += 1
                    progress = {"records_written": written, "total_records": total,
                                "bytes_written": size, "snapshot_seq": snapshot}
                    feedback(progress)
                    if batch_gate is not None:
                        batch_gate(progress, publication)
                output.flush()
                os.fsync(output.fileno())
            connection.execute("COMMIT")
            if before_publish is not None:
                before_publish(publication)
            with publication.lock:
                publication.check(stop)
                # Atomic no-clobber publication. A hard link exposes the already
                # fsynced full file; a reused goal ID never overwrites an export.
                try:
                    os.link(temporary, final)
                except FileExistsError as error:
                    raise BusinessError("EXPORT_EXISTS", "An export for this goal_id already exists.") from error
                publication.published = True
                temporary.unlink()
                # Windows does not support fsync on directory descriptors.
                # The complete file is flushed before atomic NTFS publication.
                if os.name != "nt":
                    descriptor = os.open(self.exports, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
                    try:
                        os.fsync(descriptor)
                    finally:
                        os.close(descriptor)
            if after_publish is not None:
                after_publish(publication)
            self.audit.write("export_published", goal_id=goal_id, commit_seq=snapshot,
                             records=written, bytes=size)
            return {"file": "exports/" + final.name, "records": written, "bytes": size,
                    "sha256": digest.hexdigest(), "snapshot_seq": snapshot,
                    "instance_id": self.instance_id}
        finally:
            try:
                if connection is not None:
                    connection.close()
                if temporary.exists():
                    temporary.unlink()
            finally:
                with publication.lock:
                    publication.closed = True
                    publication.closed_event.set()
                with self._lock:
                    self._active_exports -= 1

    def close(self):
        with self._lock:
            if self._active_exports:
                raise BusinessError("STORAGE_ERROR", "Stop and join active exports before closing the store.")
            if self._connection is not None:
                self._connection.close()
                self._connection = None
            if self._file_lock is not None:
                os.close(self._file_lock)
                self._file_lock = None
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
