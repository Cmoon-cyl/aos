"""Independent mutation checks for the fixed TLS source distribution."""
import copy
import json
from pathlib import Path
import shutil
import tempfile
import unittest

import check_zenoh_tls_vendor as check


class TlsSourceTests(unittest.TestCase):
    root = Path(__file__).resolve().parents[1]

    @classmethod
    def setUpClass(cls):
        cls.archive = check.find_archive()
        cls.manifest = json.loads((cls.root / check.MANIFEST_PATH).read_text())
        cls.final = check.hashes(check.tree_files(cls.root / "vendor/zenoh-link-tls"))

    def test_original_archive_and_complete_patch_replay(self):
        self.assertTrue(check.verify(self.root, self.archive)["patch_reproduces_final_tree"])

    def test_modified_unlisted_source_rejected_even_if_manifest_updated(self):
        manifest, final = copy.deepcopy(self.manifest), self.final.copy()
        final["Cargo.toml"] = "0" * 64
        manifest["final_files"] = final.copy()
        manifest["changed_original_files"].append("Cargo.toml")
        with self.assertRaisesRegex(ValueError, "declared change set"):
            check.check_change_set(manifest, final)

    def test_license_cannot_be_replaced_by_mutating_manifest(self):
        manifest, final = copy.deepcopy(self.manifest), self.final.copy()
        final["LICENSE"] = "0" * 64
        manifest["final_files"] = final.copy()
        with self.assertRaisesRegex(ValueError, "license hash"):
            check.check_change_set(manifest, final)

    def test_removed_or_unexpected_file_rejected(self):
        for name, remove in [("Cargo.toml", True), ("secret.key", False)]:
            final = self.final.copy()
            if remove:
                del final[name]
            else:
                final[name] = "0" * 64
            with self.assertRaisesRegex(ValueError, "source file set"):
                check.check_change_set(self.manifest, final)

    def test_rehashed_patch_still_must_reproduce_every_byte(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            shutil.copytree(self.root / "vendor/zenoh-link-tls", root / "vendor/zenoh-link-tls")
            patch = root / check.PATCH_PATH
            patch.parent.mkdir(parents=True)
            patch.write_bytes((self.root / check.PATCH_PATH).read_bytes() + b"\ndiff --git a/extra b/extra\nnew file mode 100644\n--- /dev/null\n+++ b/extra\n@@ -0,0 +1 @@\n+unlisted\n")
            manifest = copy.deepcopy(self.manifest)
            manifest["source_patch"]["sha256"] = check.digest(patch.read_bytes())
            (root / check.MANIFEST_PATH).write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, "complete final tree"):
                check.verify(root, self.archive)


if __name__ == "__main__":
    unittest.main()
