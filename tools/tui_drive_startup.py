#!/usr/bin/env python3
"""Check visible startup and cancellation against a local, delayed SSM server.

Usage: python3 tools/tui_drive_startup.py target/release/don
No AWS account or real secrets are used. All assertions inspect settled screens.
"""

import fcntl
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from tui_emulator import Session


class SsmServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, delay, fail):
        super().__init__(("127.0.0.1", 0), SsmHandler)
        self.delay = delay
        self.fail = fail
        self.requests = 0
        self.active = 0
        self.peak = 0
        self.lock = threading.Lock()


class SsmHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        names = json.loads(self.rfile.read(int(self.headers["Content-Length"])))["Names"]
        with self.server.lock:
            self.server.requests += 1
            self.server.active += 1
            self.server.peak = max(self.server.peak, self.server.active)
        try:
            time.sleep(self.server.delay)
            body = json.dumps({
                "Parameters": [] if self.server.fail else [
                    {"Name": name, "Value": "mock-secret-value", "Type": "SecureString"}
                    for name in names
                ],
                "InvalidParameters": names if self.server.fail else [],
            }).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/x-amz-json-1.1")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass  # Expected when startup is cancelled.
        finally:
            with self.server.lock:
                self.server.active -= 1


def wait_screen(session, predicate, timeout):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        alive = session.pump(0.01)
        session.settle(quiet=0.01, timeout=0.1)
        if predicate(session.text()):
            return True
        if not alive:
            break
    return False


def run_case(binary, name, cancel=False, fail=False):
    server = SsmServer(5 if cancel else 0.5, fail)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    os.environ.update(
        AWS_ACCESS_KEY_ID="mock",
        AWS_SECRET_ACCESS_KEY="mock",
        AWS_REGION="us-east-1",
        AWS_EC2_METADATA_DISABLED="true",
        AWS_ENDPOINT_URL_SSM=f"http://127.0.0.1:{server.server_port}",
    )
    for key in ("AWS_SESSION_TOKEN", "AWS_PROFILE"):
        os.environ.pop(key, None)
    try:
        with tempfile.TemporaryDirectory(prefix="don-startup-") as root:
            project = Path(root)
            config = 'min_version = "0.8.5"\n'
            config += '[services.probe]\nrun = { cmd = "sleep", args = ["60"] }\n'
            config += '[[secrets]]\naws-ssm = { region = "us-east-1" }\n[secrets.vars]\n'
            config += ''.join(f'TEST_{i} = "/mock/{i}"\n' for i in range(99))
            (project / "don.toml").write_text(config)
            log = project / ".don/logs/runner.log"
            log.parent.mkdir(parents=True)
            log.write_text("OLD RUN MUST NOT APPEAR\n")
            start = time.monotonic()
            session = Session(binary, root, args=("start", "--no-daemon"))
            runner_pid = None
            try:
                assert wait_screen(session, lambda s: "Starting Don" in s, 1), session.text()
                first_frame = time.monotonic() - start
                assert "OLD RUN MUST NOT APPEAR" not in session.text()
                assert wait_screen(session, lambda s: "fetching secrets..." in s, 1), session.text()
                runner_pid = int((project / ".don/don.pid").read_text().split()[0])
                assert not (project / ".don/don.sock").exists(), "API came up before secrets resolved"
                if cancel:
                    stopped = time.monotonic()
                    session.interrupt()
                    assert session.wait_exit(2) is not None, "Ctrl+C stuck on SSM"
                    elapsed = time.monotonic() - stopped
                elif fail:
                    assert session.wait_exit(3) == 1, "SSM error did not fail startup"
                    elapsed = time.monotonic() - start
                    session.settle()
                    assert "AWS SSM parameters not found" in session.text(), session.text()
                else:
                    assert wait_screen(session, lambda s: "all services running" in s, 5), session.text()
                    elapsed = time.monotonic() - start
                    assert server.requests == 10, server.requests
                    assert 1 < server.peak <= 4, server.peak
                    assert "mock-secret-value" not in session.text()
                    session.interrupt()
                    assert session.wait_exit(5) == 0, "normal shutdown failed"
                session.settle()
                assert not session.screen.alt, "alternate screen was not restored"
                # The PID file persists; it is the flock that marks a live runner.
                with (project / ".don/don.pid").open() as pid_file:
                    fcntl.flock(pid_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    fcntl.flock(pid_file, fcntl.LOCK_UN)
                print(f"{name}: first frame {first_frame:.3f}s; completion/interrupt {elapsed:.3f}s; "
                      f"SSM requests {server.requests}, peak {server.peak}", flush=True)
            finally:
                session.kill()
                os.close(session.fd)
                # Failure of an assertion must not strand this test's runner.
                if runner_pid is None:
                    try:
                        runner_pid = int((project / ".don/don.pid").read_text().split()[0])
                    except (OSError, ValueError, IndexError):
                        pass
                if runner_pid is not None:
                    try:
                        os.kill(runner_pid, signal.SIGINT)
                    except ProcessLookupError:
                        pass
    finally:
        server.shutdown()
        server.server_close()


def main():
    binary = str(Path(sys.argv[1]).resolve())
    for name, cancel, fail in [("success", False, False), ("cancel", True, False), ("failure", False, True)]:
        run_case(binary, name, cancel, fail)
    print("RESULT: ok")


if __name__ == "__main__":
    main()
