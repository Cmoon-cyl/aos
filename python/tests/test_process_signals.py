"""Installed SDK signal policy stays confined to a managed main thread."""
import os
import subprocess
import sys
import unittest


class ProcessSignalPolicyTest(unittest.TestCase):
    def test_unmanaged_and_worker_threads_preserve_signal_policy(self):
        code = '''
import os, signal, threading
from aos_sdk._process_signals import install_managed_console_handler
target = getattr(signal, "SIGBREAK", signal.SIGINT)
signal.signal(target, signal.SIG_DFL)
os.environ.pop("AOS_PROCESS_CONFIG", None)
install_managed_console_handler()
assert signal.getsignal(target) == signal.SIG_DFL
os.environ["AOS_PROCESS_CONFIG"] = "test-only-worker-guard"
worker = threading.Thread(target=install_managed_console_handler)
worker.start()
worker.join(2)
assert not worker.is_alive()
assert signal.getsignal(target) == signal.SIG_DFL
'''
        result = subprocess.run([sys.executable, "-I", "-c", code],
                                capture_output=True, text=True, timeout=5,
                                env={k: v for k, v in os.environ.items() if not k.startswith("AOS_")})
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
