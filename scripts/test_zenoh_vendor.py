"""Adversarial source-gate checks against the pinned, original crate archive."""

import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from check_zenoh_vendor import PATCH_PATH, digest, find_archive, verify


class ZenohVendorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = Path(__file__).resolve().parents[1]
        cls.archive = find_archive()

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        shutil.copytree(self.source / "vendor/zenoh", self.root / "vendor/zenoh")
        self.manifest_path = self.root / "upstream/zenoh-1.10.1-vendor-manifest.json"
        self.manifest_path.parent.mkdir()
        shutil.copy2(self.source / self.manifest_path.relative_to(self.root), self.manifest_path)
        destination = self.root / PATCH_PATH
        destination.parent.mkdir()
        shutil.copy2(self.source / PATCH_PATH, destination)
        self.manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))

    def save_manifest(self):
        self.manifest_path.write_text(json.dumps(self.manifest), encoding="utf-8")

    def change_file_and_refresh_hash(self, name, data):
        (self.root / "vendor/zenoh" / name).write_bytes(data)
        self.manifest["final_files"][name] = digest(data)
        self.save_manifest()

    def test_complete_original_archive_patch_and_tree_are_reproducible(self):
        result = verify(self.root, self.archive)
        self.assertEqual((result["original_files"], result["final_files"]), (163, 168))
        self.assertTrue(result["patch_reproduces_final_tree"])

    def test_unapproved_original_edit_fails_even_when_manifest_hash_is_updated(self):
        self.change_file_and_refresh_hash("src/lib.rs", b"unapproved original edit\n")
        with self.assertRaisesRegex(ValueError, "modified original file set"):
            verify(self.root, self.archive)

    def test_new_file_cannot_be_authorized_only_by_editing_the_manifest(self):
        self.manifest["added_files"].append("extra.rs")
        self.change_file_and_refresh_hash("extra.rs", b"unapproved addition\n")
        with self.assertRaisesRegex(ValueError, "added file set"):
            verify(self.root, self.archive)

    def test_manifest_cannot_rewrite_the_original_archive_inventory(self):
        self.manifest["original_files"].pop("README.md")
        self.manifest["final_files"].pop("README.md")
        (self.root / "vendor/zenoh/README.md").unlink()
        self.save_manifest()
        with self.assertRaisesRegex(ValueError, "original archive contents"):
            verify(self.root, self.archive)

    def test_allowed_edit_still_has_to_be_reproduced_by_the_committed_patch(self):
        name = "src/api/session.rs"
        data = (self.root / "vendor/zenoh" / name).read_bytes() + b"\n// missing patch hunk\n"
        self.change_file_and_refresh_hash(name, data)
        with self.assertRaisesRegex(ValueError, "does not reproduce"):
            verify(self.root, self.archive)

    def test_patch_hash_refresh_does_not_make_an_incomplete_patch_valid(self):
        patch_path = self.root / PATCH_PATH
        patch_path.write_bytes(b"")
        self.manifest["source_patch"]["sha256"] = digest(b"")
        self.save_manifest()
        with self.assertRaisesRegex(ValueError, "does not apply"):
            verify(self.root, self.archive)

    def test_upstream_license_bytes_remain_pinned(self):
        self.change_file_and_refresh_hash("LICENSE", b"modified license\n")
        with self.assertRaisesRegex(ValueError, "upstream license hash"):
            verify(self.root, self.archive)

    def test_corrupt_archive_is_rejected_before_any_patch_process(self):
        archive = self.root / "corrupt.crate"
        archive.write_bytes(b"not the pinned source")
        with patch("check_zenoh_vendor.subprocess.run") as process:
            with self.assertRaisesRegex(ValueError, "original archive hash"):
                verify(self.root, archive)
        process.assert_not_called()


if __name__ == "__main__":
    unittest.main()
