import json

from groupagent.cli import main


def test_configuration_error_does_not_print_input_secrets(tmp_path, capsys):
    path = tmp_path / "private.json"
    path.write_text(json.dumps({"api_key": "SYNTHETIC_SECRET_CANARY"}))
    assert main(["check-config", str(path)]) == 2
    assert "SYNTHETIC_SECRET_CANARY" not in capsys.readouterr().out


def test_schema_export_is_real_and_parseable(tmp_path):
    assert main(["schema", "--output", str(tmp_path)]) == 0
    event = json.loads((tmp_path / "event.schema.json").read_text())
    assert "source_key" in event["properties"]
    assert "platform_message_id" in event["properties"]
