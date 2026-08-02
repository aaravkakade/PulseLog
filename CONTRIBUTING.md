# Contributing

## Building

```bash
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j
ctest --test-dir build --output-on-failure
```

CMake ≥ 3.20 and a C++20 compiler. GoogleTest and Google Benchmark are fetched
at configure time; `-DPULSELOG_OFFLINE=ON` builds the engine without them.

## Before opening a pull request

```bash
# Formatting
clang-format -i $(git ls-files '*.cc' '*.h')

# Warnings as errors, as CI runs it
cmake -S . -B build-strict -DPULSELOG_WERROR=ON && cmake --build build-strict -j
ctest --test-dir build-strict --output-on-failure

# Sanitizers (ASan and TSan are mutually exclusive, hence two directories)
cmake -S . -B build-asan -DPULSELOG_SANITIZER=address+undefined
cmake --build build-asan -j && ctest --test-dir build-asan --output-on-failure

cmake -S . -B build-tsan -DPULSELOG_SANITIZER=thread
cmake --build build-tsan -j && ctest --test-dir build-tsan --output-on-failure

# End to end
scripts/smoke_test.sh
scripts/failure_test.sh
```

## Style

The `.clang-format` and `.clang-tidy` files are the authority. Beyond them:

**Comments explain why, not what.** `// increment the counter` is noise. A
comment earning its place says why a non-obvious choice was made — and the
codebase is full of those, because most of them were only obvious in
retrospect.

**Errors are values.** No exceptions on any data path. Fallible operations
return `Status` or `Result<T>`. Every `Result` is `[[nodiscard]]`.

**Ownership is explicit.** `unique_ptr` for sole ownership, references for
borrowing, raw pointers only for non-owning views whose lifetime is documented
at the declaration. No owning raw pointers, no `shared_ptr` where a single
owner will do.

**Queues are bounded.** Always. An unbounded queue turns a transient imbalance
into an out-of-memory kill.

**No detached threads.** Every thread is owned by a component that joins it.

**Don't claim what you haven't measured.** Do not describe a structure as
lock-free unless it is, do not call a copy-through-user-space path zero-copy,
and do not add an optimisation without a benchmark showing it helped. The
project has a benchmark harness precisely so this is cheap.

## Adding a protocol operation

1. Add the opcode in `include/pulselog/protocol/opcode.h`. **Never reuse or
   renumber an existing value** — retire by leaving a gap.
2. Add request/response structs in `messages.h` with `Encode`/`Decode`.
3. Implement the codec in the matching `messages_*.cc`.
4. Add round-trip, truncation and fuzz tests to `test_messages.cc`. Every
   message has all three.
5. Handle it in `Broker::HandleInline` (no partition needed) or
   `Broker::Execute` (partition-scoped).
6. Document the byte layout in `docs/PROTOCOL.md`. That file is normative.

Fields are appended, never reordered or removed. Requests must decode to
exactly zero remaining bytes; responses tolerate trailing fields.

## Adding a benchmark

Micro-benchmarks go in `benchmarks/`, one per claim made in the source or the
docs. If the benchmark stops supporting the claim, the claim changes — not the
benchmark. End-to-end scenarios go in `scripts/run_benchmarks.py`.

## Tests

* `tests/unit/` — one module, no sockets or files where avoidable
* `tests/integration/` — real in-process brokers with real sockets and files

Prefer asserting a conserved quantity (nothing lost, nothing duplicated,
offsets dense) over asserting a timing. Concurrency tests should be
deterministic in *outcome* even though the interleaving is not.
