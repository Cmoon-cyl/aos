"""Checks the installed wheel, not the source tree.

Run from a directory outside the repository with the target environment's
interpreter, for example ``python -I -m unittest discover -s <repo>/python/tests``.
"""

import importlib.machinery
import importlib.metadata
import pathlib
import sysconfig
import unittest

import aos_sdk
import aos_sdk.exceptions
from aos_sdk import _native

EXCEPTIONS = (
    "InvalidArgumentError",
    "NotFoundError",
    "CommunicationError",
    "WaitTimeoutError",
    "InvalidStateError",
)


class NativeImportTest(unittest.TestCase):
    def test_native_module_is_installed_extension(self):
        self.assertIsInstance(_native.__loader__, importlib.machinery.ExtensionFileLoader)
        native_path = pathlib.Path(_native.__file__).resolve()
        site_packages = pathlib.Path(sysconfig.get_paths()["platlib"]).resolve()
        self.assertTrue(
            native_path.is_relative_to(site_packages / "aos_sdk"),
            f"{native_path} is not under {site_packages}",
        )
        package_path = pathlib.Path(aos_sdk.__file__).resolve()
        self.assertEqual(package_path.parent, native_path.parent)

    def test_version_matches_distribution_metadata(self):
        self.assertEqual(aos_sdk.__version__, importlib.metadata.version("aos-sdk"))
        self.assertEqual(aos_sdk.__version__, _native.__version__)

    def test_typing_files_are_installed(self):
        package_dir = pathlib.Path(aos_sdk.__file__).parent
        self.assertTrue((package_dir / "py.typed").is_file())
        self.assertTrue((package_dir / "_native.pyi").is_file())


class HandleContractTest(unittest.TestCase):
    def test_handles_are_native_classes(self):
        for name in ("Context", "Publisher", "Subscription"):
            with self.subTest(name=name):
                cls = getattr(aos_sdk, name)
                self.assertIs(cls, getattr(_native, name))
                self.assertEqual(cls.__module__, "aos_sdk")
                self.assertIn(name, aos_sdk.__all__)

    def test_entities_are_created_by_a_context(self):
        for cls in (aos_sdk.Publisher, aos_sdk.Subscription):
            with self.subTest(name=cls.__name__), self.assertRaises(TypeError):
                cls()

    def test_context_options_are_keyword_only(self):
        with self.assertRaises(TypeError):
            aos_sdk.Context(0)


class ExceptionContractTest(unittest.TestCase):
    def test_public_names_are_native_classes(self):
        for name in ("AOSError",) + EXCEPTIONS:
            with self.subTest(name=name):
                cls = getattr(aos_sdk, name)
                self.assertIs(cls, getattr(_native, name))
                self.assertIs(cls, getattr(aos_sdk.exceptions, name))
                self.assertEqual(cls.__module__, "aos_sdk")
                self.assertIn(name, aos_sdk.__all__)

    def test_categories_share_base_and_stay_distinct(self):
        self.assertTrue(issubclass(aos_sdk.AOSError, Exception))
        for name in EXCEPTIONS:
            cls = getattr(aos_sdk, name)
            self.assertIs(cls.__base__, aos_sdk.AOSError, name)
        for name in EXCEPTIONS:
            for other in EXCEPTIONS:
                if name != other:
                    self.assertFalse(
                        issubclass(getattr(aos_sdk, name), getattr(aos_sdk, other)),
                        f"{name} must not be caught as {other}",
                    )

    def test_timeout_is_not_communication_failure(self):
        with self.assertRaises(aos_sdk.WaitTimeoutError) as caught:
            try:
                raise aos_sdk.WaitTimeoutError("wait ended")
            except aos_sdk.CommunicationError:
                self.fail("WaitTimeoutError was caught as CommunicationError")
        self.assertIsInstance(caught.exception, aos_sdk.AOSError)
        self.assertEqual(str(caught.exception), "wait ended")


if __name__ == "__main__":
    unittest.main()
