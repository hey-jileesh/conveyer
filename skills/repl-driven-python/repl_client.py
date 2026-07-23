#!/usr/bin/env python3
"""Minimal persistent-REPL client for LLM agents.

Keeps a single Jupyter kernel alive across shell invocations so that an
agent (whose every shell call is a fresh process) can still do
REPL-driven development: state persists in the kernel, each `eval` call
sends a snippet and prints the result.

Usage:
    python repl_client.py start            # start (or restart) the kernel
    python repl_client.py eval "1 + 1"     # evaluate code, print output
    python repl_client.py eval -f snippet.py   # evaluate a file's contents
    python repl_client.py stop             # shut the kernel down

Requires: pip install jupyter_client ipykernel
"""

import json
import os
import signal
import subprocess
import sys
import time

RUNTIME_DIR = os.environ.get("REPL_RUNTIME_DIR", "/tmp/llm-repl")
CONN_FILE = os.path.join(RUNTIME_DIR, "kernel.json")
PID_FILE = os.path.join(RUNTIME_DIR, "kernel.pid")
LOG_FILE = os.path.join(RUNTIME_DIR, "kernel.log")

AUTORELOAD_SETUP = "%load_ext autoreload\n%autoreload 2"


def _kernel_alive():
    if not (os.path.exists(PID_FILE) and os.path.exists(CONN_FILE)):
        return False
    try:
        with open(PID_FILE) as f:
            pid = int(f.read().strip())
        os.kill(pid, 0)
        return True
    except (OSError, ValueError):
        return False


def start():
    os.makedirs(RUNTIME_DIR, exist_ok=True)
    if _kernel_alive():
        print(f"kernel already running (connection: {CONN_FILE})")
        return
    for path in (CONN_FILE, PID_FILE):
        if os.path.exists(path):
            os.remove(path)
    log = open(LOG_FILE, "ab")
    proc = subprocess.Popen(
        [sys.executable, "-m", "ipykernel_launcher", "-f", CONN_FILE],
        stdout=log, stderr=log,
        start_new_session=True,  # survive the parent shell exiting
        cwd=os.getcwd(),
    )
    with open(PID_FILE, "w") as f:
        f.write(str(proc.pid))
    # wait for the connection file to appear
    for _ in range(100):
        if os.path.exists(CONN_FILE) and os.path.getsize(CONN_FILE) > 0:
            break
        time.sleep(0.1)
    else:
        sys.exit("kernel failed to start; see " + LOG_FILE)
    time.sleep(0.5)  # let the kernel finish binding its sockets
    _eval(AUTORELOAD_SETUP, quiet=True)
    print(f"kernel started (pid {proc.pid}), autoreload enabled")


def stop():
    if not os.path.exists(PID_FILE):
        print("no kernel running")
        return
    try:
        with open(PID_FILE) as f:
            pid = int(f.read().strip())
        os.kill(pid, signal.SIGTERM)
        print(f"kernel {pid} stopped")
    except (OSError, ValueError):
        print("kernel already gone")
    for path in (CONN_FILE, PID_FILE):
        if os.path.exists(path):
            os.remove(path)


def _eval(code, quiet=False, timeout=120):
    from jupyter_client import BlockingKernelClient

    client = BlockingKernelClient(connection_file=CONN_FILE)
    client.load_connection_file()
    client.start_channels()
    try:
        msg_id = client.execute(code)
        status = "ok"
        while True:
            msg = client.get_iopub_msg(timeout=timeout)
            if msg["parent_header"].get("msg_id") != msg_id:
                continue
            mtype = msg["msg_type"]
            content = msg["content"]
            if mtype == "stream" and not quiet:
                sys.stdout.write(content["text"])
            elif mtype in ("execute_result", "display_data") and not quiet:
                text = content.get("data", {}).get("text/plain")
                if text is not None:
                    print(text)
            elif mtype == "error":
                status = "error"
                if not quiet:
                    print("\n".join(content["traceback"]), file=sys.stderr)
            elif mtype == "status" and content["execution_state"] == "idle":
                break
        return status
    finally:
        client.stop_channels()


def cmd_eval(argv):
    if not _kernel_alive():
        sys.exit("no kernel running — run: python repl_client.py start")
    if argv and argv[0] == "-f":
        with open(argv[1]) as f:
            code = f.read()
    elif argv:
        code = " ".join(argv)
    else:
        code = sys.stdin.read()
    status = _eval(code)
    sys.exit(0 if status == "ok" else 1)


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ("start", "stop", "eval"):
        sys.exit(__doc__)
    cmd = sys.argv[1]
    if cmd == "start":
        start()
    elif cmd == "stop":
        stop()
    else:
        cmd_eval(sys.argv[2:])


if __name__ == "__main__":
    main()
