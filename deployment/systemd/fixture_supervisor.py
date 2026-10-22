#!/usr/bin/env python3
"""System-manager test fixture, deliberately not an SDK/AOS implementation."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


def append(path, value):
    with Path(path).open("a") as stream:
        stream.write(json.dumps(value) + "\n")
        stream.flush()
    print(json.dumps(value), flush=True)


if sys.argv[1:] == ["--escaped-child"]:
    # A new process group/session still belongs to the service's cgroup.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    print(json.dumps({"fixture_child_ready": os.getpid()}), flush=True)
    while True:
        time.sleep(1)

if sys.argv[1:4] != ["launch", "start", "--request"] or len(sys.argv) != 5:
    raise SystemExit(2)
request = json.loads(Path(sys.argv[4]).read_text())
fixture = request["plan"]["processes"][0]["env"]
output = fixture["FIXTURE_RECORD"]
mode = fixture["FIXTURE_MODE"]
if mode == "failure":
    append(output, {"event": "failure", "pid": os.getpid(), "status": 1})
    raise SystemExit(1)

child = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "--escaped-child"],
                         start_new_session=True)
append(output, {"event": "started", "pid": os.getpid(), "child": child.pid,
                "mode": mode, "request": sys.argv[4], "cwd": os.getcwd()})
stopping = False


def stop(_signum, _frame):
    global stopping
    stopping = True


signal.signal(signal.SIGTERM, signal.SIG_IGN if mode == "timeout" else stop)
signal.signal(signal.SIGINT, signal.SIG_IGN if mode == "timeout" else stop)
while not stopping:
    time.sleep(0.05)
child.kill()
child.wait(timeout=5)
append(output, {"event": "stopped", "pid": os.getpid(), "child": child.pid})
raise SystemExit(143)
