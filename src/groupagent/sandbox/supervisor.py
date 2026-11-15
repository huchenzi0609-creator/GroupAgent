"""Trusted stdlib-only POSIX supervisor installed in the read-only worker image.

This module is not a host sandbox. Docker supplies mount/network/user/cgroup
isolation; this child supplies process-group deadlines, RLIMITs and output checks.
It is invoked only as the image entrypoint, never on an untrusted host workspace.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import os
import resource
import selectors
import signal
import stat
import subprocess
import sys
import threading
import time
from pathlib import Path, PurePosixPath

PROGRAMS = {
    "python3": "/usr/local/bin/python3",
    "sh": "/bin/sh",
    "ls": "/bin/ls",
    "cat": "/bin/cat",
    "wc": "/usr/bin/wc",
    "head": "/usr/bin/head",
    "tail": "/usr/bin/tail",
    "sort": "/usr/bin/sort",
    "tesseract": "/usr/bin/tesseract",
    "soffice": "/usr/bin/soffice",
    "pdftotext": "/usr/bin/pdftotext",
    "pdftoppm": "/usr/bin/pdftoppm",
    "ffmpeg": "/usr/bin/ffmpeg",
    "ffprobe": "/usr/bin/ffprobe",
    "chromium": "/usr/bin/chromium",
}


def resource_usage(*, cgroup_root: Path = Path("/sys/fs/cgroup")) -> dict[str, int]:
    """Read worker-local counters; the optional root is a trusted test seam."""
    usage: dict[str, int] = {}

    def counter(*names: str) -> int | None:
        for name in names:
            try:
                value = int((cgroup_root / name).read_text().strip())
                if value >= 0:
                    return value
            except (OSError, ValueError):
                pass
        return None

    memory_peak = counter("memory.peak", "memory/memory.max_usage_in_bytes")
    if memory_peak is not None:
        usage["memory_peak_bytes"] = memory_peak
    pids_peak = counter("pids.peak", "pids/pids.peak")
    if pids_peak is not None:
        usage["pids_peak"] = pids_peak
    else:
        pids_current = counter("pids.current", "pids/pids.current")
        if pids_current is not None:
            usage["pids_current"] = pids_current
    try:
        cpu = dict(line.split() for line in (cgroup_root / "cpu.stat").read_text().splitlines())
        cpu_usec = int(cpu["usage_usec"])
        if cpu_usec >= 0:
            usage["cpu_usage_usec"] = cpu_usec
    except (OSError, ValueError, KeyError):
        pass
    if "cpu_usage_usec" not in usage:
        cpu_ns = counter("cpu,cpuacct/cpuacct.usage", "cpuacct/cpuacct.usage")
        if cpu_ns is not None:
            usage["cpu_usage_usec"] = cpu_ns // 1000
    for path, name in (("/work", "workspace_used_bytes"), ("/tmp", "temp_used_bytes")):
        try:
            info = os.statvfs(path)
        except OSError:
            continue
        usage[name] = (info.f_blocks - info.f_bfree) * info.f_frsize
    return usage


def relative_path(value: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if (
        not value
        or path.is_absolute()
        or "\\" in value
        or "\x00" in value
        or any(part in {"", ".", ".."} for part in value.split("/"))
    ):
        raise ValueError("invalid relative path")
    return path


def regular_file(root: Path, name: str, limit: int) -> Path:
    relative = relative_path(name)
    cursor = root
    for part in relative.parts:
        cursor /= part
        if cursor.is_symlink():
            raise ValueError("symlink output refused")
    info = cursor.stat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > limit:
        raise ValueError("non-private regular file or oversized output")
    if not cursor.resolve().is_relative_to(root.resolve()):
        raise ValueError("path outside workspace")
    return cursor


def stop_process(process: subprocess.Popen) -> None:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait(timeout=3)


def stop_descendants() -> None:
    # This process owns an otherwise empty container PID namespace. The trusted
    # supervisor is root; user commands run as 65532 and cannot signal/read it.
    for _ in range(3):
        for entry in Path("/proc").iterdir():
            if entry.name.isdigit() and int(entry.name) not in {1, os.getpid(), os.getppid()}:
                try:
                    os.kill(int(entry.name), signal.SIGKILL)
                except ProcessLookupError:
                    pass
        time.sleep(0.01)


def execute(request: dict) -> dict:
    spec, limits = request["spec"], request["limits"]
    if spec["program"] not in PROGRAMS or not Path(PROGRAMS[spec["program"]]).is_file():
        raise ValueError("program unavailable")
    work = Path("/work")
    total = 0
    if len(spec["input_paths"]) > limits["max_files"]:
        raise ValueError("too many inputs")
    if [entry["path"] for entry in request["inputs"]] != list(dict.fromkeys(spec["input_paths"])):
        raise ValueError("input manifest mismatch")
    for entry in request["inputs"]:
        name, size = entry["path"], entry["size_bytes"]
        if type(size) is not int or size < 0:
            raise ValueError("invalid input size")
        total += size
        if total > limits["max_input_bytes"]:
            raise ValueError("inputs exceed budget")
        target = work / relative_path(name)
        target.parent.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256()
        with target.open("xb") as destination:
            remaining = size
            while remaining:
                chunk = sys.stdin.buffer.read(min(65536, remaining))
                if not chunk:
                    raise ValueError("incomplete input transfer")
                destination.write(chunk)
                digest.update(chunk)
                remaining -= len(chunk)
        if digest.hexdigest() != entry["sha256"]:
            raise ValueError("input hash mismatch")
        os.chown(target, 65532, 65532)
    for name in spec["output_paths"]:
        (work / relative_path(name)).parent.mkdir(parents=True, exist_ok=True)
    for directory, _, _ in os.walk(work):
        os.chown(directory, 65532, 65532)
    Path("/tmp/home").mkdir(exist_ok=True)
    env = {
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "HOME": "/tmp/home",
        "TMPDIR": "/tmp",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
    }

    def constrain() -> None:
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        resource.setrlimit(resource.RLIMIT_NOFILE, (256, 256))
        resource.setrlimit(
            resource.RLIMIT_FSIZE, (limits["max_file_bytes"], limits["max_file_bytes"])
        )
        cpu = max(1, math.ceil(spec["timeout_seconds"]))
        resource.setrlimit(resource.RLIMIT_CPU, (cpu, cpu + 1))
        os.setgroups([])
        os.setgid(65532)
        os.setuid(65532)

    input_file = Path("/tmp/process-stdin")
    input_file.write_bytes(spec["stdin"].encode("utf-8"))
    with input_file.open("rb") as source_stdin:
        process = subprocess.Popen(
            [PROGRAMS[spec["program"]], *spec["args"]],
            cwd=work,
            env=env,
            stdin=source_stdin,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
            preexec_fn=constrain,
        )
    assert process.stdout and process.stderr
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ, "stdout")
    selector.register(process.stderr, selectors.EVENT_READ, "stderr")
    buffers = {"stdout": bytearray(), "stderr": bytearray()}
    controller_lost = threading.Event()

    def watch_controller() -> None:
        try:
            if not os.read(0, 1):
                controller_lost.set()
        except OSError:
            controller_lost.set()

    threading.Thread(target=watch_controller, daemon=True).start()
    deadline = time.monotonic() + spec["timeout_seconds"]
    timed_out = output_limited = False
    try:
        while selector.get_map():
            if controller_lost.is_set():
                stop_process(process)
                break
            if time.monotonic() >= deadline:
                timed_out = True
                stop_process(process)
                break
            for key, _ in selector.select(timeout=min(0.1, max(0, deadline - time.monotonic()))):
                chunk = os.read(key.fd, 8192)
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                available = spec["max_output_bytes"] - sum(len(value) for value in buffers.values())
                buffers[key.data].extend(chunk[: max(0, available)])
                if len(chunk) > available:
                    output_limited = True
                    stop_process(process)
                    break
            if output_limited:
                break
        if process.poll() is None:
            remaining = deadline - time.monotonic()
            try:
                process.wait(timeout=max(0, remaining))
            except subprocess.TimeoutExpired:
                timed_out = True
                stop_process(process)
    finally:
        selector.close()
        # Kill descendants even when the direct child already returned.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        stop_descendants()
    outputs = []
    if process.returncode == 0 and not (timed_out or output_limited or controller_lost.is_set()):
        total = 0
        for name in spec["output_paths"]:
            path = regular_file(work, name, limits["max_file_bytes"])
            data = path.read_bytes()
            total += len(data)
            if total > limits["max_artifact_bytes"]:
                raise ValueError("outputs exceed budget")
            outputs.append(
                {
                    "path": name,
                    "sha256": hashlib.sha256(data).hexdigest(),
                    "size_bytes": len(data),
                    "data": base64.b64encode(data).decode("ascii"),
                }
            )
    return {
        "exit_code": process.returncode,
        "stdout": bytes(buffers["stdout"]).decode("utf-8", errors="replace"),
        "stderr": bytes(buffers["stderr"]).decode("utf-8", errors="replace"),
        "timed_out": timed_out,
        "cancelled": controller_lost.is_set(),
        "output_limited": output_limited,
        "outputs": outputs,
        "resource_usage": resource_usage(),
    }


def main() -> int:
    try:
        raw = sys.stdin.buffer.readline(2_000_001)
        if len(raw) > 2_000_000:
            raise ValueError("request too large")
        result = execute(json.loads(raw))
    except Exception:
        # Never return exception repr, host paths, environment, or private inputs.
        print(json.dumps({"error": "worker_failed"}))
        return 2
    print(json.dumps(result, ensure_ascii=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
