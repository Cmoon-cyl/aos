"""Portable ownership and pipe helpers for installed-process acceptance."""

import os
import queue
import signal
import subprocess
import threading


def ensure_console():
    if os.name == "nt":
        import ctypes
        kernel = ctypes.windll.kernel32
        if kernel.GetConsoleCP() == 0:
            if not kernel.AllocConsole():
                raise ctypes.WinError()
            from ctypes import wintypes
            kernel.GetConsoleWindow.restype = wintypes.HWND
            ctypes.windll.user32.ShowWindow.argtypes = [wintypes.HWND, ctypes.c_int]
            ctypes.windll.user32.ShowWindow(kernel.GetConsoleWindow(), 0)


def console_process_options():
    ensure_console()
    return {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP} if os.name == "nt" else {}


def interrupt_process(process, posix_signal=signal.SIGINT):
    process.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else posix_signal)


def read_line(stream, timeout):
    """Read one complete line with a deadline; select only accepts sockets on Windows."""
    result = queue.Queue()

    def read():
        try:
            result.put((stream.readline(), None))
        except BaseException as error:
            result.put((None, error))

    threading.Thread(target=read, name="acceptance-one-line", daemon=True).start()
    try:
        line, error = result.get(timeout=timeout)
    except queue.Empty:
        raise AssertionError("child emitted no complete line before the deadline") from None
    if error is not None:
        raise error
    return line


def assert_child_identity(test, identity, process):
    """Bind an interpreter's report to its owned launcher on Windows."""
    test.assertNotEqual(identity["pid"], os.getpid())
    if os.name == "nt" and identity["pid"] != process.pid:
        test.assertEqual(identity["ppid"], process.pid, identity)
    else:
        test.assertEqual(identity["pid"], process.pid, identity)


def process_alive(pid):
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel.OpenProcess.restype = wintypes.HANDLE
        kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
        kernel.WaitForSingleObject.restype = wintypes.DWORD
        kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel.CloseHandle.restype = wintypes.BOOL
        handle = kernel.OpenProcess(0x00100000, False, pid)  # SYNCHRONIZE only
        if not handle:
            error = ctypes.get_last_error()
            if error == 87:  # ERROR_INVALID_PARAMETER: no process with this PID
                return False
            raise ctypes.WinError(error)
        try:
            state = kernel.WaitForSingleObject(handle, 0)
            if state not in (0, 258):  # WAIT_OBJECT_0 / WAIT_TIMEOUT
                raise ctypes.WinError(ctypes.get_last_error())
            return state == 258
        finally:
            kernel.CloseHandle(handle)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def directory_alias(link, target):
    if os.name == "nt":
        # A directory junction needs no symlink privilege and still exercises
        # the native canonicalization/containment check against a real alias.
        subprocess.run(["cmd", "/d", "/c", "mklink", "/J", str(link), str(target)],
                       check=True, capture_output=True)
    else:
        link.symlink_to(target, target_is_directory=True)


def assert_canonical_path(actual, expected):
    """Require an absolute canonical path, tolerating Windows verbatim spelling."""
    import os
    from pathlib import Path

    actual, expected = Path(actual), Path(expected).resolve(strict=True)
    assert actual.is_absolute(), (actual, expected)
    if os.name != "nt":
        assert actual == expected, (actual, expected)
        return
    assert actual.samefile(expected), (actual, expected)

    def spelling(path):
        value = str(path)
        if value.startswith("\\\\?\\UNC\\"):
            value = "\\\\" + value[8:]
        elif value.startswith("\\\\?\\"):
            value = value[4:]
        return value.casefold()

    assert spelling(actual) == spelling(expected), (actual, expected)
