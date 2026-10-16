# Deferred dependency watches

Services with a resolved executable start after the initial Bazel build and
binary-path cquery. Dependency query, XML parsing, per-target attribution, watch
registration, and the gap scan follow in the same owned preparation worker.
All Don preparation, rebuild, and graph requery commands share the Bazel mutex.

Explicit watch patterns and workspace graph sentinels still initialize before
preparation. Tasks keep their existing explicit input watches and auto-run
policy; they do not request Bazel dependency watches. A missing cquery executable
waits for setup because its `bazel run` fallback could otherwise contend with
the deferred query.

The gap scan starts from a timestamp taken before building, and runs after
registration acknowledgements. Source changes replay the normal rebuild signal;
graph changes refresh dependency watches and invalidate pending artifacts as
well as running services. Directory mtimes cover removed and renamed inputs.
Query failure installs temporary conservative workspace coverage and probes
Bazel freshness. A later edit retries resolution; successful resolution restores
per-target attribution. This fallback can restart unrelated services while
coverage is conservative.

## Reproducing the measurements

Verbose logs separately time the query, XML parser, attribution, watch directory
traversal, watch-set reconciliation, backend registration, and gap scan. To
isolate CPU and registration work from Bazel, capture the dependency XML and
write its exact target labels, one per line, to a text file:

```sh
DON_PROFILE_XML=/path/deps.xml \
DON_PROFILE_TARGETS=/path/targets.txt \
DON_PROFILE_ROOT=/path/checkout \
cargo test --release --lib profile_dependency_watches -- --ignored --nocapture
```

The isolated profile uses no application processes or explicit source watches;
it ignores `**/node_modules/**` and `data/**`. Real startup can register fewer new
watches when explicit patterns already cover the same directories. Sum the
`traversal`, `reconcile`, and `backend` durations over registration log lines.
Backend duration includes notify/FSEvents or inotify work, not just syscalls.

For a controlled orchestration comparison, build the baseline and patched CLI
binaries and run:

```sh
python3 tools/benchmark-deferred-watches.py /path/before /path/after
```

This interleaves three starts per binary with a two-second fake query and 256
packages. It measures first child-process execution and stops only its own
runner. It is not a substitute for an application boot benchmark.

For real startup, use identical config, target set and Bazel server/cache state.
Measure from CLI launch to a verified service child PID (not a task, the runner,
or the daemon). Record cold and warm results separately, and retain the query,
build, cquery and service-spawn timestamps. A failed build is not a startup
sample. Do not mix target sets or report the isolated profile as a full boot.

## Regression coverage

`tests/deferred_watch_test.rs` runs the library runner against a controllable
Bazel executable. Barriers verify that services execute while query is blocked,
Bazel build/query commands do not overlap, gap edits/deletions/renames and BUILD
changes are replayed, watch-disabled targets stay disabled, fallback coverage
recovers after query failure, and watched tasks preserve once-only and generator
semantics. It also checks ordinary post-registration edits and shutdown during a
blocked query.

## Release ordering

The proposed release is 0.8.4. Merge the Don change and publish the tag through
its release workflow before downstream version pins change. Verify all platform
archives and checksums exist. Then bump image/bootstrap/CLI pins together and
regenerate any lockfile checksums through the downstream extension. Do not
invent release hashes or merge pins to unavailable artifacts.
