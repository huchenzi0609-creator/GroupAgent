import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from groupagent.config import AppConfig, ConfigError, load_config

EXAMPLE = Path(__file__).resolve().parents[1] / "docs/spec/config.example.json"


def test_spec_example_and_constraints():
    config = load_config(EXAMPLE)
    assert config.conversation.hard_reply_character_limit is None
    assert not config.features.video_analysis
    assert config.features.audio_transcription
    assert not config.features.speech_reply_on_request
    assert not config.runtime.early_ack_enabled
    assert not config.group.periodic_roster_refresh
    assert "engine_not_validated" in config.production_blockers()


@pytest.mark.parametrize(
    "section,key,value",
    [
        ("features", "video_analysis", True),
        ("features", "video_generation", True),
        ("features", "image_generation", True),
        ("conversation", "hard_reply_character_limit", 200),
        ("execution", "max_tool_calls", -1),
        ("execution", "heartbeat_seconds", 31),
        ("group", "time_zone", "No/SuchZone"),
        ("group", "periodic_roster_refresh", True),
        ("engine", "extra_session_log_upload", True),
        ("model_routes", "text_agent", ""),
        ("model_routes", "vision", "   "),
        ("group", "external_group_id", "   "),
        ("group", "internal_id", "   "),
        ("spend", "pricing_snapshot_id", "   "),
        ("group", "observer_ids", ["   "]),
    ],
)
def test_invalid_or_out_of_scope_config_rejected(section, key, value):
    raw = json.loads(EXAMPLE.read_text())
    raw[section][key] = value
    with pytest.raises(ValidationError):
        AppConfig.model_validate(raw)


def test_unconfigured_example_cannot_be_production(tmp_path):
    raw = json.loads(EXAMPLE.read_text())
    raw["deployment_mode"] = "production"
    path = tmp_path / "config.json"
    path.write_text(json.dumps(raw))
    with pytest.raises(ConfigError, match="Production configuration incomplete"):
        load_config(path)


def test_misspelled_option_fails_instead_of_silently_ignored():
    raw = json.loads(EXAMPLE.read_text())
    raw["features"]["video_anaylsis"] = True
    with pytest.raises(ValidationError):
        AppConfig.model_validate(raw)


def test_configured_roles_cannot_silently_overlap():
    raw = json.loads(EXAMPLE.read_text())
    raw["group"]["observer_ids"] = raw["group"]["owner_ids"]
    with pytest.raises(ValidationError, match="overlapping"):
        AppConfig.model_validate(raw)
