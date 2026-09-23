# Post-build file scan benchmark

Measured September 17, 2026 on macOS arm64 using a captured 11-target workspace. The baseline is PR #55’s first commit, `1b77ff5ec86c44e2013a0b07b58332a9786ab103`; the candidate adds batch-scoped filesystem metadata and directory-listing reuse.

| Metric | First commit | Shared batch scan |
| --- | ---: | ---: |
| Median scan | 1.541s | 0.647s |
| Range | 1.515–1.665s | 0.621–0.701s |

The shared scan won all 20 paired comparisons. Median duration decreased by 58.0%, saving about 0.89s per batch. A separate instrumented prototype measured metadata reads dropping from 236,282 to 59,611 and directory reads from 27,704 to 7,815.

## Method

The captured workload contains 22 source/BUILD scan groups, with 12,849 patterns and 3,258 unique patterns. These groups have no ignore rules; regression tests exercise differing service ignores, source/BUILD classification, symlinks, and new batches observing edits and directory changes.

Twenty paired comparisons alternate order, with one excluded warm-up for each variant. Both implementations are compiled together with `rustc -O`. Each batch starts with an empty cache, and timing includes its construction and destruction. A future cutoff forces full traversal of the unchanged tree. Tracked source inputs stayed fixed; other processes and ignored/generated files were not controlled. These numbers cover synchronous filesystem scans, not full Don startup or Tokio scheduling.

The cache is owned by the preparation batch and moves into each blocking scan worker. Each service still applies its own matching and ignore rules. Existing watch registrations continue observing edits during the scan. Cancellation discards the result; a read-only worker already running may finish in the background. No cache survives into another build batch.

Validation passed: 12 focused path/batch tests, the startup-build shutdown regression, and two isolated binary scenarios (clean startup and source edited during the build).

## Paired measurements

| Pair | First | Baseline seconds | Shared seconds |
| ---: | --- | ---: | ---: |
| 1 | baseline | 1.665320 | 0.633859 |
| 2 | shared | 1.539922 | 0.652393 |
| 3 | baseline | 1.533583 | 0.631307 |
| 4 | shared | 1.562534 | 0.700926 |
| 5 | baseline | 1.515142 | 0.641345 |
| 6 | shared | 1.533835 | 0.653950 |
| 7 | baseline | 1.546699 | 0.637972 |
| 8 | shared | 1.556612 | 0.665879 |
| 9 | baseline | 1.535208 | 0.638046 |
| 10 | shared | 1.547848 | 0.651954 |
| 11 | baseline | 1.561581 | 0.641845 |
| 12 | shared | 1.522946 | 0.664459 |
| 13 | baseline | 1.542100 | 0.627713 |
| 14 | shared | 1.540011 | 0.654877 |
| 15 | baseline | 1.568281 | 0.621062 |
| 16 | shared | 1.530233 | 0.652893 |
| 17 | baseline | 1.545040 | 0.631364 |
| 18 | shared | 1.529979 | 0.654266 |
| 19 | baseline | 1.541703 | 0.640077 |
| 20 | shared | 1.531152 | 0.660211 |

## Reproduce

Use `tools/benchmark-file-scans.rs` with captured scan groups encoded as `[name, patterns, ignores]` tuples. Paths may be absolute or relative to the workspace. For example:

```json
[["api", ["src/**"], ["src/generated/**"]], ["worker", ["src/**"], []]]
```

The original capture contains private workspace paths and is not committed. Use the same captured inputs for both variants. After building Don, point `DON_BENCH_DEPS` at its existing release dependency directory:

```bash
git show 1b77ff5ec86c44e2013a0b07b58332a9786ab103:src/process/paths.rs > /tmp/don-scan-baseline.rs
export DON_SCAN_BASELINE=/tmp/don-scan-baseline.rs
export DON_BENCH_DEPS="${CARGO_TARGET_DIR:-target}/release/deps"
python3 - <<'PY'
import os, pathlib, subprocess
deps = pathlib.Path(os.environ["DON_BENCH_DEPS"])
command = ["rustc", "--edition=2024", "-O", "-A", "dead_code",
           "tools/benchmark-file-scans.rs", "-L", f"dependency={deps}",
           "-o", "/tmp/don-scan-benchmark"]
for crate in ("glob", "serde_json"):
    libraries = list(deps.glob(f"lib{crate}-*.rlib"))
    if len(libraries) != 1:
        raise SystemExit(f"Select the current {crate} rlib from {deps}")
    command.extend(["--extern", f"{crate}={libraries[0]}"])
subprocess.run(command, check=True)
PY
/tmp/don-scan-benchmark /path/to/workspace /path/to/scan-inputs.json 20 > /tmp/don-scan-results.csv
```

The harness alternates ordering, excludes its warm-ups, and emits one CSV row per measured batch. It creates no services or Don state and performs no builds of the captured workspace.
