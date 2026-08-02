#!/usr/bin/env python3
"""Capture the machine and build a benchmark ran on.

A throughput number without its hardware is not a result, it is a rumour. This
writes everything needed to judge whether two runs are comparable: CPU, memory,
storage type, kernel, compiler, the exact optimisation flags the binaries were
built with, and the commit they were built from.

Fields that cannot be determined are recorded as null rather than guessed. A
missing disk model is honest; an invented one silently corrupts every
comparison made against the file later.

Usage:
  scripts/bench_metadata.py --build-dir build --out results/metadata.json
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shutil
import subprocess
import sys
from pathlib import Path


def run(*command: str) -> str | None:
    """Run a command, returning stripped stdout, or None if it does not work.

    Every probe here is best-effort: these tools are absent on minimal
    containers and differ between distributions.
    """
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    output = result.stdout.strip()
    return output or None


def read_file(path: str) -> str | None:
    try:
        with open(path) as handle:
            return handle.read()
    except OSError:
        return None


def cpu_model() -> str | None:
    if sys.platform == "darwin":
        return run("sysctl", "-n", "machdep.cpu.brand_string")
    cpuinfo = read_file("/proc/cpuinfo") or ""
    for key in ("model name", "Model name", "Hardware", "Model"):
        match = re.search(rf"^{key}\s*:\s*(.+)$", cpuinfo, re.MULTILINE)
        if match:
            return match.group(1).strip()
    # ARM server parts often expose only the implementer/part codes.
    part = re.search(r"^CPU part\s*:\s*(.+)$", cpuinfo, re.MULTILINE)
    return f"ARM part {part.group(1).strip()}" if part else None


def total_memory_bytes() -> int | None:
    if sys.platform == "darwin":
        value = run("sysctl", "-n", "hw.memsize")
        return int(value) if value else None
    meminfo = read_file("/proc/meminfo") or ""
    match = re.search(r"^MemTotal:\s+(\d+) kB$", meminfo, re.MULTILINE)
    return int(match.group(1)) * 1024 if match else None


def storage_info(path: Path) -> dict:
    """Describe the device the benchmark data directory lives on.

    Rotational vs solid-state changes fsync cost by orders of magnitude, so a
    durable-mode result is uninterpretable without it. On Linux this is
    readable from sysfs; elsewhere it is left unknown.
    """
    info: dict = {"path": str(path), "rotational": None, "model": None, "filesystem": None}

    filesystem = run("df", "-P", "-T", str(path)) if sys.platform != "darwin" else None
    if filesystem:
        lines = filesystem.splitlines()
        if len(lines) > 1:
            info["filesystem"] = lines[1].split()[1]

    if sys.platform == "darwin":
        # APFS on Apple Silicon is always flash; there is no rotational media
        # to distinguish and no sysfs equivalent to read.
        info["filesystem"] = info["filesystem"] or "apfs"
        info["rotational"] = False
        return info

    source = run("findmnt", "-n", "-o", "SOURCE", "--target", str(path))
    if not source or not source.startswith("/dev/"):
        return info

    device = Path(source).name
    # Strip the partition suffix: nvme0n1p1 -> nvme0n1, sda1 -> sda.
    base = re.sub(r"p?\d+$", "", device) if not device.startswith("dm-") else device
    rotational = read_file(f"/sys/block/{base}/queue/rotational")
    if rotational:
        info["rotational"] = rotational.strip() == "1"
    model = read_file(f"/sys/block/{base}/device/model")
    if model:
        info["model"] = model.strip()
    return info


def compiler_info(build_dir: Path) -> dict:
    """Read the compiler and flags out of the CMake cache, not the environment.

    The environment says what would be used for a new build; the cache says
    what these binaries were actually compiled with, which is the only thing
    the numbers depend on.
    """
    info: dict = {"id": None, "version": None, "cxx_flags": None, "build_type": None}
    cache = read_file(str(build_dir / "CMakeCache.txt"))
    if not cache:
        return info

    def cached(key: str) -> str | None:
        match = re.search(rf"^{re.escape(key)}:[A-Z]+=(.*)$", cache, re.MULTILINE)
        value = match.group(1).strip() if match else None
        return value or None

    info["build_type"] = cached("CMAKE_BUILD_TYPE")

    # The compiler identity is not a cache entry -- CMake writes it to
    # CMakeFiles/<cmake-version>/CMakeCXXCompiler.cmake as a set() call.
    for detected in build_dir.glob("CMakeFiles/*/CMakeCXXCompiler.cmake"):
        contents = read_file(str(detected)) or ""
        for key, field in (("CMAKE_CXX_COMPILER_ID", "id"),
                           ("CMAKE_CXX_COMPILER_VERSION", "version")):
            match = re.search(rf'^set\({key} "([^"]*)"\)', contents, re.MULTILINE)
            if match and match.group(1):
                info[field] = match.group(1)
        break

    compiler = cached("CMAKE_CXX_COMPILER")
    if compiler and not info["version"]:
        version = run(compiler, "--version")
        info["version"] = version.splitlines()[0] if version else None

    build_type = (info["build_type"] or "").upper()
    flags = [cached("CMAKE_CXX_FLAGS") or ""]
    if build_type:
        flags.append(cached(f"CMAKE_CXX_FLAGS_{build_type}") or "")
    combined = " ".join(part for part in flags if part).strip()
    info["cxx_flags"] = combined or None
    return info


def git_info() -> dict:
    commit = run("git", "rev-parse", "HEAD")
    status = run("git", "status", "--porcelain")
    return {
        "commit": commit,
        "short_commit": commit[:12] if commit else None,
        # A dirty tree means the binaries may not match the commit, which makes
        # the run non-reproducible. Say so rather than implying it is clean.
        "dirty": bool(status),
        "branch": run("git", "rev-parse", "--abbrev-ref", "HEAD"),
    }


def cpu_governor() -> str | None:
    """The frequency governor, which bounds how repeatable timings can be.

    'powersave' or an active boost means run-to-run variance is expected and
    the spread in the report is doing real work.
    """
    return (read_file("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor") or "").strip() or None


def virtualisation() -> str | None:
    hypervisor = run("systemd-detect-virt")
    if hypervisor and hypervisor != "none":
        return hypervisor
    if os.environ.get("GITHUB_ACTIONS") == "true":
        return "github-hosted-runner"
    return None


def collect(build_dir: Path, data_path: Path) -> dict:
    return {
        "host": {
            "os": platform.system(),
            "distro": _distro(),
            "kernel": platform.release(),
            "architecture": platform.machine(),
            "hostname": platform.node(),
            "virtualisation": virtualisation(),
        },
        "cpu": {
            "model": cpu_model(),
            "logical_cores": os.cpu_count(),
            "governor": cpu_governor(),
        },
        "memory": {"total_bytes": total_memory_bytes()},
        "storage": storage_info(data_path),
        "build": compiler_info(build_dir),
        "git": git_info(),
        "tools": {
            "cmake": (run("cmake", "--version") or "\n").splitlines()[0] or None,
            "python": platform.python_version(),
        },
    }


def _distro() -> str | None:
    if sys.platform == "darwin":
        version = run("sw_vers", "-productVersion")
        return f"macOS {version}" if version else "macOS"
    release = read_file("/etc/os-release") or ""
    match = re.search(r'^PRETTY_NAME="?([^"\n]+)"?$', release, re.MULTILINE)
    return match.group(1) if match else None


def format_human(metadata: dict) -> str:
    host, cpu = metadata["host"], metadata["cpu"]
    storage, build, git = metadata["storage"], metadata["build"], metadata["git"]
    memory = metadata["memory"]["total_bytes"]

    def unknown(value, suffix: str = "") -> str:
        return f"{value}{suffix}" if value is not None else "unknown"

    disk = "unknown"
    if storage["rotational"] is not None:
        disk = "rotational" if storage["rotational"] else "solid-state"
        if storage["model"]:
            disk = f"{disk} ({storage['model']})"
    if storage["filesystem"]:
        disk = f"{disk}, {storage['filesystem']}"

    lines = [
        f"  cpu      {unknown(cpu['model'])}, {unknown(cpu['logical_cores'])} logical cores",
        f"  memory   {f'{memory / 2**30:.1f} GiB' if memory else 'unknown'}",
        f"  storage  {disk}",
        f"  os       {unknown(host['distro'])}, kernel {unknown(host['kernel'])} ({unknown(host['architecture'])})",
        f"  compiler {unknown(build['id'])} {unknown(build['version'])} [{unknown(build['build_type'])}]",
        f"  flags    {unknown(build['cxx_flags'])}",
        f"  commit   {unknown(git['short_commit'])}{' (DIRTY TREE)' if git['dirty'] else ''}",
    ]
    if host["virtualisation"]:
        lines.append(f"  virt     {host['virtualisation']} -- shared hardware, expect wider spread")
    if cpu["governor"] and cpu["governor"] != "performance":
        lines.append(f"  governor {cpu['governor']} -- frequency is not pinned")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--build-dir", default="build", type=Path)
    parser.add_argument("--data-path", default=None, type=Path,
                        help="a path on the filesystem the brokers will write to")
    parser.add_argument("--out", default=None, type=Path)
    args = parser.parse_args()

    data_path = args.data_path or Path(os.environ.get("TMPDIR", "/tmp"))
    metadata = collect(args.build_dir, data_path)

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        with open(args.out, "w") as handle:
            json.dump(metadata, handle, indent=2)
    print(format_human(metadata))
    if metadata["git"]["dirty"]:
        print("\nwarning: the working tree has uncommitted changes; this run is not "
              "reproducible from the recorded commit", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
