"""Default console policy, called only after Context validates a launch bootstrap."""
import os
import signal
import threading


def install_managed_console_handler():
    if (os.name == "nt" and os.environ.get("AOS_PROCESS_CONFIG")
            and threading.current_thread() is threading.main_thread()
            and signal.getsignal(signal.SIGBREAK) == signal.SIG_DFL):
        # Match normal Python Ctrl-C semantics so executor waits unwind through
        # finally/context-manager cleanup. Explicit application handlers win.
        signal.signal(signal.SIGBREAK, signal.default_int_handler)
