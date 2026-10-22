"""Negative tests for the source archive evidence boundary."""

import hashlib
import importlib.util
import io
import os
from pathlib import Path
import tarfile
import tempfile
import unittest

_spec = importlib.util.spec_from_file_location("aos_sdist", Path(__file__).with_name("check_sdist.py"))
_sdist = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_sdist)


class SdistArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.contents = {
            "Cargo.toml": b"[workspace]\n",
            "vendor/zenoh/src/api/session.rs": b"const PATCH: u32 = 1;\n",
            "scripts/check": b"#!/bin/sh\nexit 0\n",
        }
        self.expected = {
            name: {"sha256": hashlib.sha256(data).hexdigest(), "executable": name == "scripts/check"}
            for name, data in self.contents.items()
        }

    def archive(self, contents=None, extra=None, modes=None):
        path = self.root / "source.tar.gz"
        with tarfile.open(path, "w:gz") as bundle:
            for name, data in {"PKG-INFO": b"Name: aos-sdk\n", **(
                self.contents if contents is None else contents
            )}.items():
                info = tarfile.TarInfo("aos_sdk-0.2.0/" + name)
                info.size = len(data)
                info.mode = 0o755 if name == "scripts/check" else 0o644
                info.mode = (modes or {}).get(name, info.mode)
                bundle.addfile(info, io.BytesIO(data))
            if extra is not None:
                info, data = extra
                info.size = len(data)
                bundle.addfile(info, io.BytesIO(data))
        return path

    def reject(self, archive, message):
        with self.assertRaisesRegex(ValueError, message):
            _sdist.unpack_verified(archive, self.root / "unpacked", self.expected)
        self.assertFalse((self.root / "unpacked").exists())

    def test_all_source_bytes_and_executable_mode_are_preserved(self):
        result = _sdist.unpack_verified(self.archive(), self.root / "unpacked", self.expected)
        for name, data in self.contents.items():
            self.assertEqual((result / name).read_bytes(), data)
            # Windows does not retain POSIX execute bits on ordinary files.
            # Archive-mode validation below remains required on every host.
            if os.name != "nt":
                self.assertEqual(bool((result / name).stat().st_mode & 0o111), name == "scripts/check")

    def test_changed_archive_executable_mode_is_rejected(self):
        for name in self.contents:
            with self.subTest(name=name):
                mode = 0o644 if name == "scripts/check" else 0o755
                self.reject(self.archive(modes={name: mode}), "Changed executable mode")

    def test_generator_modes_are_restored_without_changing_source_bytes(self):
        archive = self.archive(modes={"scripts/check": 0o644, "Cargo.toml": 0o755})
        original_bytes = archive.read_bytes()
        original = self.root / "generator" / archive.name
        record = _sdist.restore_source_modes(archive, self.expected, original)
        self.assertEqual(original.read_bytes(), original_bytes)
        self.assertEqual({item["path"] for item in record["changes"]}, {"scripts/check", "Cargo.toml"})
        unpacked = _sdist.unpack_verified(archive, self.root / "unpacked", self.expected)
        for name, data in self.contents.items():
            self.assertEqual((unpacked / name).read_bytes(), data)

    def test_correct_archive_is_not_repacked(self):
        archive = self.archive()
        original_bytes = archive.read_bytes()
        original = self.root / "generator" / archive.name
        record = _sdist.restore_source_modes(archive, self.expected, original)
        self.assertEqual(record["changes"], [])
        self.assertEqual(archive.read_bytes(), original_bytes)
        self.assertFalse(original.exists())

    def test_mode_restoration_rejects_changed_bytes_before_repacking(self):
        archive = self.archive(dict(self.contents, **{"Cargo.toml": b"changed"}))
        original_bytes = archive.read_bytes()
        original = self.root / "generator" / archive.name
        with self.assertRaisesRegex(ValueError, "Changed source"):
            _sdist.restore_source_modes(archive, self.expected, original)
        self.assertEqual(archive.read_bytes(), original_bytes)
        self.assertFalse(original.exists())

    def test_mode_restoration_rejects_unsafe_paths_before_repacking(self):
        info = tarfile.TarInfo("aos_sdk-0.2.0/../../outside")
        archive = self.archive(extra=(info, b"not extracted"))
        original = self.root / "generator" / archive.name
        with self.assertRaisesRegex(ValueError, "Unsafe"):
            _sdist.restore_source_modes(archive, self.expected, original)
        self.assertFalse(original.exists())

    def test_missing_vendor_source_is_rejected(self):
        contents = {name: data for name, data in self.contents.items() if not name.startswith("vendor/")}
        self.reject(self.archive(contents), "file mismatch")

    def test_changed_source_is_rejected(self):
        contents = dict(self.contents, **{"Cargo.toml": b"[workspace]\nresolver = '2'\n"})
        self.reject(self.archive(contents), "Changed source")

    def test_duplicate_file_is_rejected(self):
        info = tarfile.TarInfo("aos_sdk-0.2.0/Cargo.toml")
        self.reject(self.archive(extra=(info, self.contents["Cargo.toml"])), "Duplicate")

    def test_link_is_rejected(self):
        info = tarfile.TarInfo("aos_sdk-0.2.0/linked")
        info.type = tarfile.SYMTYPE
        info.linkname = "/tmp/unrelated"
        self.reject(self.archive(extra=(info, b"")), "non-file")

    def test_traversal_is_rejected(self):
        info = tarfile.TarInfo("aos_sdk-0.2.0/../../outside")
        self.reject(self.archive(extra=(info, b"not extracted")), "Unsafe")


if __name__ == "__main__":
    unittest.main()
