"""Manage this checkout's isolated Unix-socket-only PostgreSQL test cluster."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import stat
import subprocess
from pathlib import Path
from urllib.parse import urlencode

ROOT = Path(__file__).resolve().parents[1]


def validate_paths(runtime: Path) -> None:
    """Refuse redirection of control commands or output writes through child links."""
    if runtime.is_symlink() or runtime != runtime.resolve():
        raise ValueError("Runtime path must be canonical and not a symlink")
    relatives = [
        "",
        "socket",
        "db",
        "dsn.txt",
        "server.log",
        "cluster-owner.json",
        "db/PG_VERSION",
        "db/postmaster.pid",
        "db/postmaster.opts",
        "db/postgresql.conf",
        "db/pg_hba.conf",
        "db/pg_ident.conf",
    ]
    for relative in relatives:
        path = runtime / relative
        if path.is_symlink() or path != path.resolve():
            raise ValueError("Refusing symlink in developer cluster: " + relative)
        if path.exists() and path.stat().st_uid != os.getuid():
            raise ValueError("Developer cluster path is not owned by current user: " + relative)
        if relative not in {"", "socket", "db"} and path.exists():
            info = path.stat()
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise ValueError("Expected a regular file with one link: " + relative)
    for relative in ["", "socket", "db"]:
        path = runtime / relative
        if path.exists() and not path.is_dir():
            raise ValueError("Expected a directory: " + relative)


def ownership_record(runtime: Path, port: int) -> dict:
    return {"version": 1, "project_root": str(ROOT), "data_dir": str(runtime / "db"), "port": port}


def write_private_text(path: Path, value: str) -> None:
    # Inspect the opened inode before truncating; O_NOFOLLOW alone permits hard links.
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid():
            raise ValueError("Output must be an owned regular file with one link")
        os.fchmod(fd, 0o600)
        os.ftruncate(fd, 0)
        with os.fdopen(fd, "w", encoding="utf-8", closefd=False) as output:
            output.write(value)
    finally:
        os.close(fd)


def require_owned_cluster(runtime: Path, port: int) -> None:
    marker = runtime / "cluster-owner.json"
    if not marker.is_file() or json.loads(marker.read_text()) != ownership_record(runtime, port):
        raise ValueError("This existing cluster is not registered to this checkout and port")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["start", "stop", "status", "dsn"])
    parser.add_argument("--runtime-root", type=Path, default=ROOT / "runtime/postgres")
    parser.add_argument("--port", type=int, default=55439)
    args = parser.parse_args()
    runtime = args.runtime_root.absolute()
    try:
        validate_paths(runtime)
    except ValueError as exc:
        parser.error(str(exc))
    if not 1024 <= args.port <= 65535:
        parser.error("Select an unprivileged valid port number")
    socket = runtime / "socket"
    data = runtime / "db"
    dsn = "postgresql:///groupagent_test?" + urlencode({"host": str(socket), "port": args.port})
    if args.command == "dsn":
        print(dsn)
        return 0
    pg_config = shutil.which("pg_config")
    if not pg_config:
        parser.error("Install PostgreSQL 16+ and expose pg_config on PATH")
    binary = Path(subprocess.check_output([pg_config, "--bindir"], text=True).strip())
    control = [str(binary / "pg_ctl"), "-D", str(data)]
    if (data / "PG_VERSION").exists() or args.command in {"stop", "status"}:
        try:
            require_owned_cluster(runtime, args.port)
        except (ValueError, OSError) as exc:
            parser.error(str(exc))
    if args.command == "status":
        return subprocess.call(control + ["status"])
    if args.command == "stop":
        return subprocess.call(control + ["-m", "fast", "-w", "stop"])
    if len(str(socket / f".s.PGSQL.{args.port}").encode()) > 100:
        parser.error("Unix socket path too long; select a shorter --runtime-root")
    runtime.mkdir(mode=0o700, parents=True, exist_ok=True)
    socket.mkdir(mode=0o700, exist_ok=True)
    runtime.chmod(0o700)
    socket.chmod(0o700)
    if not (data / "PG_VERSION").exists():
        subprocess.run(
            [
                str(binary / "initdb"),
                "-D",
                str(data),
                "--auth-local=trust",
                "--auth-host=reject",
                "--encoding=UTF8",
                "--locale=C",
            ],
            check=True,
        )
        marker = runtime / "cluster-owner.json"
        fd = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as output:
            json.dump(ownership_record(runtime, args.port), output)
    running = subprocess.run(control + ["status"], capture_output=True).returncode == 0
    if not running:
        # Do not expose TCP or relax authentication on any existing server.
        if any(c in str(socket) for c in ["'", '"', "\n"]):
            parser.error("Unsupported runtime path characters")
        options = f"-k '{socket}' -p {args.port} -c listen_addresses='' -c shared_buffers=64MB -c max_connections=30"
        subprocess.run(
            control + ["-l", str(runtime / "server.log"), "-o", options, "-w", "start"], check=True
        )
    common = ["-h", str(socket), "-p", str(args.port)]
    exists = subprocess.check_output(
        [
            str(binary / "psql"),
            *common,
            "-d",
            "postgres",
            "-tAc",
            "SELECT 1 FROM pg_database WHERE datname='groupagent_test'",
        ],
        text=True,
    ).strip()
    if exists != "1":
        subprocess.run([str(binary / "createdb"), *common, "groupagent_test"], check=True)
    env_file = runtime / "dsn.txt"
    write_private_text(env_file, dsn + "\n")
    print("Isolated developer database ready; DSN file:", env_file)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
