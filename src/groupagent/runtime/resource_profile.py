"""Translate the opt-in host profile into the shared transient worker contract."""

from groupagent.config import AppConfig
from groupagent.resources import ResourceGovernor
from groupagent.sandbox import SandboxLimits

# The controller is already resident in MemAvailable. This is bounded transient
# headroom for preparation, text/network work and review, not a child cgroup cap.
CONTROLLER_MEMORY_MB = 64


def controller_resources_ready(governor: ResourceGovernor) -> bool:
    # Preserve the same transfer reserve, free floor, cgroup, pressure and
    # exclusive-worker/poison checks. Actual child launches still reserve 512+ MiB.
    return governor.ready(CONTROLLER_MEMORY_MB)


def worker_limits(config: AppConfig, *, harness: bool = False) -> SandboxLimits:
    policy = config.resources
    limits = SandboxLimits(
        memory_mb=policy.worker_memory_mb,
        cpus=policy.worker_cpus,
        cpu_shares=policy.worker_cpu_shares,
        oom_score_adj=policy.worker_oom_score_adj,
        pids=policy.worker_pids,
        workspace_mb=policy.worker_workspace_mb,
        temp_mb=256 if harness and not policy.enabled else policy.worker_temp_mb,
    )
    if policy.enabled:
        return SandboxLimits.model_validate(
            {
                **limits.model_dump(),
                "max_input_bytes": config.limits.total_input_bytes_per_work,
                "max_file_bytes": config.limits.output_file_bytes,
                # A full-sized artifact also travels with a bounded JSON
                # manifest/result file; that metadata is not a user file cap.
                "max_artifact_bytes": config.limits.output_file_bytes + 1024**2,
            }
        )
    return limits


def worker_tool_names(names: frozenset[str]) -> frozenset[str]:
    """Admit before a handler mutates an artifact or reserves paid media calls."""
    return frozenset(
        name
        for name in names
        if name.startswith("document_")
        or name
        in {
            "pdf_combine",
            "chart_render",
            "python_compute",
            "process_run",
            "shell_command",
            "image_analyze",
            "image_convert",
            "media_ocr",
            "provider_ocr",
            "audio_transcribe",
            "speech_synthesize",
            "browser_open",
            "web_image_import",
            "artifact_import",
            "artifact_export",
            "artifact_from_workspace",
        }
    )
