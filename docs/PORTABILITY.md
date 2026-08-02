# Portability

PulseLog targets Linux and macOS. Linux is the deployment target and the only
platform CI validates; macOS is where development happens, and it is supported
because a build you cannot run locally is a build nobody debugs.

Those two are not equivalent, and this document says exactly where they differ.
The important cases are not the ones that fail to compile — those announce
themselves. They are the ones where the same source silently means something
weaker on one platform, and `fsync` is the worst of them.

Everything below was read out of the source, not assumed. Where a guarantee
differs by platform, the weaker guarantee is stated plainly rather than
averaged away.

---

## 1. What is actually verified where

| | Linux x86-64 | macOS ARM64 |
|---|---|---|
| Compiles with `-Werror` | GCC 13 and Clang, in CI | Apple Clang 15, locally |
| Unit + integration tests | every CI run | every local run |
| ASan + UBSan | every CI run | locally, minus leak detection |
| TSan | every CI run | locally |
| clang-tidy | every CI run | via a Linux container |
| End-to-end cluster smoke | every CI run | not run |
| Failure injection | every CI run, incl. under ASan | locally |
| Published benchmarks | yes | separate, clearly labelled section |

`ASAN_OPTIONS=detect_leaks=1` is silently unsupported by macOS ASan — the
runtime prints `detect_leaks is not supported on this platform` and the process
exits. Leak checking therefore only ever happens on Linux.

---

## 2. Durability: the difference that matters most

This is the one place where identical code provides a materially different
guarantee, so it is first.

| | Linux | macOS |
|---|---|---|
| `storage.fsync.mode=full` | `fsync(2)` | `fcntl(F_FULLFSYNC)` |
| `storage.fsync.mode=data` | `fdatasync(2)` | `fsync(2)` |

On Linux, `fsync` instructs the drive to commit; with a well-behaved drive and
no write cache lying, data survives power loss. `fdatasync` skips metadata that
does not affect readability, which is a real saving on a log that appends to a
preallocated file — the size does not change, so there is less metadata to
push.

On macOS, plain `fsync` only pushes data to the *drive's* cache and returns.
It does not survive power loss. `F_FULLFSYNC` asks the drive to commit to
stable media, which is what durability has to mean for a log, and it is
markedly more expensive. This is why `mode=full` maps to `F_FULLFSYNC` there:
the honest mapping is the slow one.

The consequence for reading results: **a macOS `mode=data` number is not
comparable to a Linux `mode=data` number.** On Linux it is `fdatasync`; on
macOS it is `fsync`, which promises less than Linux's `fsync` does. Both are
weaker than `F_FULLFSYNC`. Durable-mode benchmarks are reported per platform
and never averaged.

`F_FULLFSYNC` is rejected by some filesystems and by every network mount;
`FileHandle::Sync` falls back to `fsync` on `ENOTSUP`/`EINVAL` rather than
failing the write. On such a mount, `mode=full` on macOS quietly provides only
what `fsync` provides.

---

## 3. Event loop

| | Linux | macOS / FreeBSD | anything else |
|---|---|---|---|
| Readiness | `epoll` | `kqueue` | compile error |
| Loop wakeup | `eventfd` | self-pipe | self-pipe |

Both backends implement the same `Poller` interface and the rest of the engine
does not know which is in use. The broker logs `poller=epoll` or
`poller=kqueue` at start-up, and CI asserts the Linux build actually selected
`epoll` — a Linux build that somehow chose kqueue would pass every test and
validate nothing.

An unsupported platform is a `#error`, not a silent fallback to `poll(2)`.
Falling back would produce a broker that works and is slow for reasons nobody
could see.

`eventfd` is one descriptor and an 8-byte counter; the self-pipe is two
descriptors with per-byte bookkeeping. The difference is negligible next to a
wakeup's cost, but the descriptor count matters at high loop counts.

---

## 4. Sockets

| Concern | Linux | macOS |
|---|---|---|
| Suppress `SIGPIPE` | `MSG_NOSIGNAL` per send | `SO_NOSIGPIPE` per socket |
| `EAGAIN` vs `EWOULDBLOCK` | same value | same value |
| `SO_REUSEPORT` | available | available |
| Send/receive buffer autotuning | yes, up to `tcp_wmem`/`tcp_rmem` | more conservative defaults |

`SIGPIPE` is also ignored process-wide, so a path that misses both mechanisms
still cannot kill the broker.

`EAGAIN` and `EWOULDBLOCK` are required to be distinct by POSIX and are the
same value on both platforms here. Testing both is the portable idiom, but
written directly it is a tautology GCC reports under `-Wlogical-op`;
`IsWouldBlock()` in `socket.cc` keeps the portability without the warning.

**Kernel buffer autotuning is a real behavioural difference.** Linux grows a
socket's send buffer up to `net.ipv4.tcp_wmem`'s maximum — 4 MiB on a stock
kernel — per socket. This is why `Connection.ClosesWhenOutputCeilingIsExceeded`
passed on macOS and failed on Linux: the kernel absorbed every response the
test produced, the userspace queue never filled, and the ceiling under test
never engaged. Both backpressure tests now set a small send buffer explicitly.
Operationally the same effect means the userspace output ceiling bounds only
what PulseLog queues, not what the kernel holds on its behalf.

---

## 5. Filesystem

| Concern | Linux | macOS |
|---|---|---|
| Preallocation | `posix_fallocate` | `fcntl(F_PREALLOCATE)` + `ftruncate` |
| Preallocation unsupported | falls back to `ftruncate` | falls back to `ftruncate` |
| Typical filesystem | ext4, xfs | APFS |

`posix_fallocate` returning `EOPNOTSUPP` or `ENOSYS` is not an error — some
filesystems do not implement it. Both paths finish with `ftruncate` so the file
size is visible either way; on a filesystem without real preallocation the
result is a sparse file, which still avoids the metadata updates that come from
growing a file on every append, but does not reserve blocks.

There are no in-place checkpoint rewrites anywhere in the engine, so no atomic
rename is required. Consumer group offsets are stored in an append-only log
using the same segment machinery as topic data, replayed at start-up. Torn
tails are handled by the same CRC-and-truncate recovery path rather than by a
rename dance.

---

## 6. Threads

| Concern | Linux | macOS |
|---|---|---|
| Naming | `pthread_setname_np(thread, name)` | `pthread_setname_np(name)` |
| Name length limit | 15 chars + NUL | 63 chars |
| CPU pinning | `pthread_setaffinity_np` | **not supported** |

The two `pthread_setname_np` signatures differ, not just the limits: Linux
names any thread, macOS only the caller. Thread names are truncated to each
platform's limit rather than failing, so `pl-worker-0` shows up under both
`top -H` and Instruments.

**CPU pinning does not work on macOS.** The platform exposes only affinity
*hints* through `thread_policy_set`, and Apple silicon ignores them.
`TryPinCurrentThread` returns `false` there rather than pretending to succeed,
so a caller can log that pinning was unavailable instead of assuming a layout
it did not get. Any benchmark that depends on pinning is therefore Linux-only,
and none of the published numbers use it.

---

## 7. Compiler and architecture

**CRC-32C** has three implementations, selected at compile time:

| Architecture | Implementation |
|---|---|
| x86-64 with SSE 4.2 | `_mm_crc32_u64` / `_mm_crc32_u8` |
| aarch64 with `__ARM_FEATURE_CRC32` | `__crc32cd` / `__crc32cw` / `__crc32ch` / `__crc32cb` |
| anything else | table-driven software |

All three produce identical output — they compute the same polynomial, and the
tests check the accelerated path against the software one rather than trusting
that they agree. The broker reports which is active in its metadata, so a
result computed on the software path cannot be mistaken for a hardware one.

**Endianness**: every multi-byte integer on the wire and on disk is
little-endian, chosen because both target architectures already are, so the
common case is a plain load. Big-endian hosts byteswap via
`__builtin_bswap{16,32,64}`. No code relies on unaligned access. Big-endian is
untested — there is no CI runner for it — and is written to be correct rather
than claimed to be verified.

**Type identity** differs in a way that produces contradictory warnings:
`std::size_t` is `unsigned long` and `std::uint64_t` is `unsigned long long` on
macOS (distinct types), while on 64-bit Linux they are the same type. The same
`static_cast` is therefore *required* by Apple Clang's `-Wconversion` and
*rejected* by GCC's `-Wuseless-cast`. `Narrow<To, From>()` in `base/types.h`
routes the conversion through a template so the types stay dependent, which
satisfies both while keeping the narrowing explicit and greppable.

**`strerror_r`** has GNU and POSIX variants with different return types.
`status.cc` selects on `_GNU_SOURCE`.

---

## 8. Process metrics

| | Linux | macOS |
|---|---|---|
| Resident memory | `/proc/self/status` | `mach_task_basic_info` |
| CPU time | `/proc/self/stat` | `getrusage` |
| Bytes written to disk | `/proc/self/io` | **not available** |

Per-process write counters have no macOS equivalent without elevated
privileges. `bench_cluster_metrics.py` reports `bytes_written: null` there
rather than estimating one — an invented figure would silently corrupt every
comparison made against the file afterwards.

---

## 9. Things that are not portable and are not claimed to be

- **Windows** is not supported and is not a `#error` away from working: the
  engine assumes POSIX sockets, `fork`-free process management, POSIX file
  semantics and pthreads throughout.
- **io_uring** is not used. The event loop is `epoll` on Linux.
- **Big-endian hosts** are handled in code but never tested.
- **32-bit** builds are not supported. Offsets are `int64_t` and segment
  positions assume a 64-bit `size_t`.

---

## 10. Adding platform-specific code

Two rules, both learned from the differences above:

1. **Put it behind an interface that names the capability, not the platform.**
   `Poller` is the model: callers ask for readiness, not for epoll. A
   `#if defined(__linux__)` scattered through calling code makes the two
   platforms diverge in behaviour without anyone noticing.

2. **When the guarantee differs, say so at the call site and here.** The
   `fsync` case is the reason for this rule. `Sync(SyncMode::kData)` compiles
   and runs on both platforms and means something weaker on one of them; only a
   comment at the implementation and a row in this document make that visible.

An unsupported platform should be a compile error. Silent degradation produces
a system that works, is slower or less durable than documented, and gives
nobody a reason to look.
