#!/usr/bin/env python3
"""Interleave two Don binaries on an isolated, controlled Bazel workload.

This measures orchestration, not a real application boot. For real graph phase
costs use the ignored Rust profile_dependency_watches test with captured XML.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time


def run(binary, delay, packages):
    with tempfile.TemporaryDirectory(prefix="don-watch-bench-") as directory:
        root = Path(directory)
        (root / "MODULE.bazel").touch()
        (root / "bin").mkdir()
        (root / "bazel-out/bin").mkdir(parents=True)
        graph = ["<query>"]
        for i in range(packages):
            (root / f"p{i}").mkdir()
            (root / f"p{i}/source").write_text("input")
            graph.append(f'<source-file name="//p{i}:source"/>')
        for name in ("a", "b"):
            inputs = "".join(f'<rule-input name="//p{i}:source"/>' for i in range(packages))
            graph.append(f'<rule name="//{name}:app">{inputs}</rule>')
            script = root / f"bazel-out/bin/{name}"
            script.write_text(f"#!/bin/sh\ntouch {name}-started\nexec sleep 60\n")
            script.chmod(0o755)
        graph.append("</query>")
        (root / "graph.xml").write_text("".join(graph))
        fake = root / "bin/bazel"
        fake.write_text(f'''#!/bin/sh
case "$1" in
version) exit 0 ;;
build) sleep .1 ;;
cquery) printf '//a:app\\tbazel-out/bin/a\\n//b:app\\tbazel-out/bin/b\\n' ;;
query) sleep {delay}; cat graph.xml ;;
esac
''')
        fake.chmod(0o755)
        (root / "don.toml").write_text('''[services.a]
bazel.target = "//a:app"
[services.b]
bazel.target = "//b:app"
''')
        env = dict(os.environ, PATH=str(root / "bin") + os.pathsep + os.environ.get("PATH", ""))
        with (root / "output").open("wb") as log:
            started = time.monotonic()
            process = subprocess.Popen([binary, "start", "--no-tui", "--no-daemon", "-v"], cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT)
            try:
                while not any((root / f"{n}-started").exists() for n in ("a", "b")):
                    if process.poll() is not None or time.monotonic() - started > 60:
                        raise RuntimeError((root / "output").read_text())
                    time.sleep(.005)
                elapsed = time.monotonic() - started
            finally:
                if process.poll() is None:
                    process.send_signal(signal.SIGINT)
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.send_signal(signal.SIGINT)
                        process.wait(timeout=10)
        return {"binary": binary, "first_service_seconds": round(elapsed, 4), "query_delay_seconds": delay, "packages": packages}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("before")
    parser.add_argument("after")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--query-delay", type=float, default=2)
    parser.add_argument("--packages", type=int, default=256)
    args = parser.parse_args()
    for iteration in range(args.repeats):
        for label in ("before", "after"):
            result = run(str(Path(getattr(args, label)).resolve()), args.query_delay, args.packages)
            print(json.dumps(dict(result, variant=label, iteration=iteration + 1)), flush=True)
