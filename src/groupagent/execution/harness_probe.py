"""Read-only Harness prerequisite probe; never starts a model or executes a tool.

Run directly with Python's standard library or with -m groupagent.execution.harness_probe.
Exit 2 means the adoption gate has not passed. Installing a package alone does not pass it.
"""

from __future__ import annotations

import importlib.metadata
import importlib.util
import json
import platform
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

HARNESS_REFERENCE_COMMIT = "39163dd30f703a79079e2d2446545cff3c453738"


def installed_cli_metadata() -> dict[str, str | None]:
    """Read the discovered npm package manifest, without launching dsh or reading its home."""
    executable = shutil.which("dsh")
    result: dict[str, str | None] = {"executable": executable, "npm_package_version": None}
    if not executable:
        return result
    manifest = Path(executable).resolve().parent.parent / "package.json"
    try:
        package = json.loads(manifest.read_text())
        if package.get("name") == "@deepseek-ai/dsh" and isinstance(package.get("version"), str):
            result["npm_package_version"] = package["version"]
    except (OSError, ValueError, AttributeError):
        pass
    return result


def probe() -> dict[str, Any]:
    distributions: dict[str, str | None] = {}
    for name in ("deepseek-harness-sdk", "deepseek-harness-runtime-bin"):
        try:
            distributions[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            distributions[name] = None
    blockers = []
    if not distributions["deepseek-harness-sdk"]:
        blockers.append("sdk_distribution_missing")
    if not distributions["deepseek-harness-runtime-bin"]:
        blockers.append("matching_runtime_distribution_missing")
    if distributions["deepseek-harness-sdk"] != distributions["deepseek-harness-runtime-bin"]:
        blockers.append("sdk_runtime_version_mismatch")
    docker_available = shutil.which("docker") is not None
    if not docker_available:
        blockers.append("docker_cli_unavailable_no_os_isolation_test")
    # These require explicit experiments and recorded evidence, not automatic inference
    # from a successful import or binary presence.
    blockers.extend(
        [
            "installed_distribution_not_verified_against_reference_commit",
            "scoped_plugin_composition_not_verified",
            "session_log_contribution_disable_not_wire_verified",
            "process_per_work_cancel_and_recovery_not_verified",
            "worker_credential_path_network_isolation_not_verified",
        ]
    )
    return {
        "observed_at": datetime.now(UTC).isoformat(),
        "reference_commit": HARNESS_REFERENCE_COMMIT,
        "python": platform.python_version(),
        "platform": platform.system(),
        "machine": platform.machine(),
        "distributions": distributions,
        "sdk_module_discoverable": importlib.util.find_spec("deepseek_harness") is not None,
        "dsh_cli_discoverable": shutil.which("dsh") is not None,
        "installed_dsh_cli": installed_cli_metadata(),
        "docker_cli_discoverable": docker_available,
        "node_cli_discoverable": shutil.which("node") is not None,
        "credentials_read": False,
        "runtime_started": False,
        "model_requests": 0,
        "adoption_gate_passed": False,
        "blockers": blockers,
    }


def main() -> int:
    print(json.dumps(probe(), ensure_ascii=False, indent=2))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
