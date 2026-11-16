"""Pure configuration contracts for the opt-in 2 GiB host profile."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest
from pydantic import ValidationError

from groupagent.config import AppConfig, LimitsConfig, ResourceConfig, RuntimeConfig, load_config

MIB = 1024**2
EXAMPLES = Path(__file__).resolve().parents[1] / "docs/spec"


def config_data(**sections):
    return {
        "group": {
            "internal_id": "synthetic-resource-group",
            "external_group_id": "synthetic-resource-channel",
            "owner_ids": ["synthetic-owner"],
        },
        **sections,
    }


def test_standard_profile_keeps_existing_engine_and_media_budgets():
    config = AppConfig.model_validate(config_data())
    assert config.resources.profile == "standard" and not config.resources.enabled
    assert config.runtime.engine == "harness"
    assert config.resources.worker_memory_mb == 1024
    assert config.resources.worker_cpus == 1
    assert config.resources.worker_cpu_shares == 1024
    assert config.resources.worker_oom_score_adj == 0
    assert config.resources.worker_pids == 256
    assert config.resources.worker_workspace_mb == 512
    assert config.resources.worker_temp_mb == 128
    assert config.resources.maintenance_interval_seconds == 60
    assert config.limits.image_bytes == 15 * MIB
    assert config.limits.image_pixels == 40_000_000
    assert config.limits.images_per_work == 10
    assert config.limits.office_or_pdf_bytes == 30 * MIB
    assert config.limits.input_files_per_work == 20
    assert config.limits.total_input_bytes_per_work == 100 * MIB
    assert config.limits.workspace_bytes == 512 * MIB


def test_2gb_profile_selects_a_single_scoped_worker_and_smaller_input_limits():
    config = AppConfig.model_validate(config_data(resources={"profile": "server_2gb"}))
    assert config.resources.enabled
    assert config.runtime.engine == "scoped"
    assert config.execution.max_concurrent_heavy_work == 1
    assert config.resources.worker_memory_mb == 512
    assert config.resources.worker_cpus == 0.75
    assert config.resources.worker_cpu_shares == 128
    assert config.resources.worker_oom_score_adj == 500
    assert config.resources.worker_pids == 192
    assert config.resources.worker_workspace_mb == 128
    assert config.resources.worker_temp_mb == 64
    assert config.resources.reserve_memory_mb == 128
    assert config.resources.transfer_memory_mb == 128
    assert config.resources.retry_seconds == 5
    assert config.resources.maintenance_interval_seconds == 180
    assert config.limits.image_bytes == 8 * MIB
    assert config.limits.image_pixels == 12_000_000
    assert config.limits.images_per_work == 4
    assert config.limits.office_or_pdf_bytes == 16 * MIB
    assert config.limits.input_files_per_work == 8
    assert config.limits.total_input_bytes_per_work == 32 * MIB
    assert config.limits.workspace_bytes == 128 * MIB
    assert config.limits.output_file_bytes == 25 * MIB


def test_2gb_defaults_preserve_explicit_resource_and_input_overrides():
    config = AppConfig.model_validate(
        config_data(
            resources={
                "profile": "server_2gb",
                "worker_memory_mb": 640,
                "worker_cpus": 0.5,
                "reserve_memory_mb": 160,
                "retry_seconds": 7,
                "maintenance_interval_seconds": 300,
            },
            runtime={"engine": "scoped", "bot_aliases": ["组长"]},
            limits={"image_bytes": 4 * MIB, "output_file_bytes": 20 * MIB},
        )
    )
    assert config.resources.worker_memory_mb == 640
    assert config.resources.worker_cpus == 0.5
    assert config.resources.reserve_memory_mb == 160
    assert config.resources.retry_seconds == 7
    assert config.resources.maintenance_interval_seconds == 300
    assert config.resources.worker_pids == 192
    assert config.runtime.bot_aliases == ["组长"]
    assert config.limits.image_bytes == 4 * MIB
    assert config.limits.output_file_bytes == 20 * MIB
    assert config.limits.office_or_pdf_bytes == 16 * MIB


def test_nested_models_receive_profile_defaults_only_for_unset_fields():
    config = AppConfig.model_validate(
        config_data(
            resources=ResourceConfig(profile="server_2gb", worker_memory_mb=640),
            runtime=RuntimeConfig(bot_aliases=["组长"]),
            limits=LimitsConfig(image_bytes=4 * MIB),
        )
    )
    assert config.resources.worker_memory_mb == 640
    assert config.runtime.engine == "scoped"
    assert config.runtime.bot_aliases == ["组长"]
    assert config.limits.image_bytes == 4 * MIB
    assert config.limits.office_or_pdf_bytes == 16 * MIB


def test_applying_profile_does_not_modify_the_supplied_configuration():
    raw = config_data(
        resources={"profile": "server_2gb"},
        runtime={"bot_aliases": ["组长"]},
        limits={"image_bytes": 4 * MIB},
    )
    original = deepcopy(raw)
    AppConfig.model_validate(raw)
    assert raw == original


@pytest.mark.parametrize(
    "sections",
    [
        {"runtime": {"engine": "harness"}},
        {"execution": {"max_concurrent_heavy_work": 2}},
    ],
)
def test_2gb_profile_rejects_resident_harness_or_multiple_heavy_work(sections):
    with pytest.raises(ValidationError, match="2GB profile requires scoped execution"):
        AppConfig.model_validate(config_data(resources={"profile": "server_2gb"}, **sections))


def test_a_smaller_standard_worker_does_not_enable_host_admission():
    config = AppConfig.model_validate(
        config_data(resources={"profile": "standard", "worker_memory_mb": 512})
    )
    assert not config.resources.enabled
    assert config.runtime.engine == "harness"
    assert config.limits.workspace_bytes == 512 * MIB


@pytest.mark.parametrize(
    "field,value",
    [
        ("profile", "2gb"),
        ("worker_memory_mb", 511),
        ("worker_memory_mb", 512.0),
        ("worker_cpus", 0),
        ("worker_pids", 31),
        ("worker_oom_score_adj", 1001),
        ("retry_seconds", 0),
        ("worker_memroy_mb", 512),
    ],
)
def test_unsafe_or_misspelled_resource_values_are_rejected(field, value):
    with pytest.raises(ValidationError):
        ResourceConfig.model_validate({"profile": "server_2gb", field: value})


@pytest.mark.parametrize(
    "filename,profile,engine",
    [
        ("config.example.json", "standard", "harness"),
        ("config.server-2gb.example.json", "server_2gb", "scoped"),
    ],
)
def test_examples_validate_with_explicit_pot_identity_and_closed_release_gates(
    filename, profile, engine
):
    config = load_config(EXAMPLES / filename)
    assert config.resources.profile == profile
    assert config.runtime.engine == engine
    assert config.runtime.bot_name == "Pot" and config.runtime.bot_badge == "【Pot】"
    assert {"机器人", "机器人组长", "组长", "小伴"} <= set(config.runtime.bot_aliases)
    assert config.runtime.channel == "local" and not config.runtime.allow_live_send
    assert config.limits.output_file_bytes == 25 * MIB
    assert "group_disabled" in config.production_blockers()
    assert "spend_limit_or_price_snapshot_missing" in config.production_blockers()
