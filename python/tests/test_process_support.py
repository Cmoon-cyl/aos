"""Keep process-acceptance helpers strict across native host platforms."""

import io
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import process_support as support


class ProcessSupportTest(unittest.TestCase):
    def test_canonical_path_rejects_relative_and_noncanonical_aliases(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory).resolve() / "resource"
            target.write_text("fixture")
            support.assert_canonical_path(target, target)
            relative = Path(os.path.relpath(target))
            noncanonical = target.parent / ".." / target.parent.name / target.name
            for alias in (relative, noncanonical):
                self.assertTrue(alias.samefile(target))
                with self.subTest(alias=alias), self.assertRaises(AssertionError):
                    support.assert_canonical_path(alias, target)
            if os.name == "nt":
                support.assert_canonical_path("\\\\?\\" + str(target), target)

    def test_worker_identity_requires_the_owned_launcher(self):
        process = mock.Mock(pid=100)
        with mock.patch.object(support.os, "name", "nt"):
            support.assert_child_identity(self, {"pid": 101, "ppid": 100}, process)
            with self.assertRaises(AssertionError):
                support.assert_child_identity(self, {"pid": 101, "ppid": 999}, process)
        with mock.patch.object(support.os, "name", "posix"):
            with self.assertRaises(AssertionError):
                support.assert_child_identity(self, {"pid": 101, "ppid": 100}, process)
            support.assert_child_identity(self, {"pid": 100}, process)

    def test_liveness_distinguishes_running_and_reaped_processes(self):
        self.assertTrue(support.process_alive(os.getpid()))
        child = subprocess.Popen([sys.executable, "-I", "-c", "pass"])
        self.assertEqual(child.wait(10), 0)
        self.assertFalse(support.process_alive(child.pid))

    def test_one_line_reader_keeps_eof_and_read_errors(self):
        stream = io.StringIO('first\nsecond\n')
        self.assertEqual(support.read_line(stream, 1), 'first\n')
        self.assertEqual(support.read_line(stream, 1), 'second\n')
        self.assertEqual(support.read_line(stream, 1), '')
        stream.close()
        with self.assertRaises(ValueError):
            support.read_line(stream, 1)
