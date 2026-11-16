#!/usr/bin/env python3
"""Extract only matched musl PG clients/libs from a verified, local PG image.

Runs as the trusted maintainer, not as the agent. Compatible with host Python 3.6.
No credentials or database files are read; the extraction container never starts.
"""

import os
import re
import subprocess
import sys
from pathlib import Path


def main():
    image = sys.argv[1]
    if not re.fullmatch(r"sha256:[a-f0-9]{64}", image):
        raise SystemExit("postgres_image_invalid")
    output = subprocess.check_output(
        [
            "docker",
            "run",
            "--rm",
            "--network=none",
            "--read-only",
            "--memory=64m",
            "--cpus=0.25",
            "--pids-limit=32",
            "--entrypoint",
            "sh",
            image,
            "-c",
            "ldd /usr/local/bin/pg_dump; ldd /usr/local/bin/pg_restore",
        ],
        universal_newlines=True,
    )
    paths = {"/usr/local/bin/pg_dump", "/usr/local/bin/pg_restore"}
    paths.update(re.findall(r"(/(?:usr/)?(?:local/)?lib/[^\s()]+)", output))
    if "/lib/ld-musl-x86_64.so.1" not in paths:
        raise SystemExit("postgres_musl_runtime_required")
    prefix = Path("/opt/pot-test2/postgresql")
    runtime = prefix / "runtime"
    prefix.joinpath("bin").mkdir(parents=True, exist_ok=True)
    name = "pot-test2-pg-client-extract"
    subprocess.check_call(
        ["docker", "create", "--name", name, "--network=none", "--entrypoint", "sh", image],
        stdout=subprocess.DEVNULL,
    )
    try:
        for path in sorted(paths):
            if ".." in Path(path).parts:
                raise SystemExit("postgres_library_path_invalid")
            target = runtime / path.lstrip("/")
            target.parent.mkdir(parents=True, exist_ok=True)
            subprocess.check_call(["docker", "cp", "-L", name + ":" + path, str(target)])
    finally:
        subprocess.check_call(["docker", "rm", name], stdout=subprocess.DEVNULL)
    library_path = ":".join(str(runtime / path) for path in ("lib", "usr/lib", "usr/local/lib"))
    for tool in ("pg_dump", "pg_restore"):
        wrapper = prefix / "bin" / tool
        wrapper.write_text(
            "#!/bin/sh\nexec "
            + str(runtime / "lib/ld-musl-x86_64.so.1")
            + " --library-path "
            + library_path
            + " "
            + str(runtime / "usr/local/bin" / tool)
            + ' "$@"\n'
        )
        os.chmod(str(wrapper), 0o755)
        subprocess.check_call([str(wrapper), "--version"])
    print("postgres_clients_prepared")


if __name__ == "__main__":
    main()
