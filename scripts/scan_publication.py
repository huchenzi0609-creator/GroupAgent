"""Scan only publishable Git files, never private runtime/secrets, with Gitleaks."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import tempfile
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", type=Path, default=Path("runtime/secret-audit.json"))
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    report = args.report.absolute()
    report.parent.mkdir(parents=True, exist_ok=True)
    paths = subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=root
    ).split(b"\0")
    with tempfile.TemporaryDirectory(prefix="groupagent-publication-") as temporary:
        destination = Path(temporary)
        count = 0
        for raw in paths:
            if not raw:
                continue
            path = root / raw.decode()
            if path.is_symlink():
                raise RuntimeError("Publishable files must not redirect to private state")
            if not path.is_file():
                continue
            target = destination / path.relative_to(root)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, target)
            count += 1
        result = subprocess.run(
            [
                "gitleaks",
                "dir",
                str(destination),
                "--redact",
                "--config",
                str(root / ".gitleaks.toml"),
                "--report-format",
                "json",
                "--report-path",
                str(report),
                "--no-banner",
            ],
            check=False,
        )
        print(json.dumps({"publication_files": count, "scanner_exit": result.returncode}))
        return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
