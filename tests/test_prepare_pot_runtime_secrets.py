"""Synthetic bytes and local metadata only; never execute the root helper."""

import importlib.util
import json
import os
import stat
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "deploy/prepare_pot_runtime_secrets.py"
SPEC = importlib.util.spec_from_file_location("prepare_pot_runtime_secrets", SCRIPT)
assert SPEC and SPEC.loader
helper = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(helper)


def test_payload_quotes_values_accepts_one_native_terminator_and_rejects_invalid_lines():
    config = json.dumps({"api_key": 'synthetic-"quoted\\key'}).encode()
    for token in (b"synthetic-token", b"synthetic-token\n", b"synthetic-token\r\n"):
        entries = dict(
            line.split("=", 1) for line in helper.payload(config, token).decode().splitlines()
        )
        assert set(entries) == {"DEEPSEEK_API_KEY", "GROUPAGENT_CHANNEL_TOKEN"}
        assert json.loads(entries["DEEPSEEK_API_KEY"]) == 'synthetic-"quoted\\key'
        assert json.loads(entries["GROUPAGENT_CHANNEL_TOKEN"]) == "synthetic-token"
    for bad in (b"", b" \n", b"synthetic\n\n", b"synthetic\r", b"two\nlines", b"nul\x00token"):
        with pytest.raises(helper.PreparationError) as error:
            helper.payload(config, bad)
        assert error.value.code == "credential_value_invalid"
    for bad in ("", " ", "two\nlines", "nul\x00key"):
        with pytest.raises(helper.PreparationError):
            helper.payload(json.dumps({"api_key": bad}).encode(), b"synthetic-token")


def test_source_rejects_links_or_public_permissions_and_tmpfs_uses_tightest_mount(tmp_path):
    source = tmp_path / "synthetic-source"
    source.write_bytes(b"synthetic-private")
    source.chmod(0o600)
    assert helper.private_read(source) == b"synthetic-private"
    source.chmod(0o640)
    with pytest.raises(helper.PreparationError):
        helper.private_read(source)
    source.chmod(0o600)
    alias = tmp_path / "symlink"
    alias.symlink_to(source)
    with pytest.raises(helper.PreparationError):
        helper.private_read(alias)
    linked = tmp_path / "hardlink"
    os.link(source, linked)
    with pytest.raises(helper.PreparationError):
        helper.private_read(source)
    assert source.read_bytes() == b"synthetic-private"
    mountinfo = tmp_path / "synthetic-mountinfo"
    mountinfo.write_text("1 0 1:1 / / rw - ext4 device rw\n2 1 0:1 / /run rw - tmpfs tmpfs rw\n")
    helper.require_tmpfs(Path("/run/pot-test2"), mountinfo)
    mountinfo.write_text(mountinfo.read_text() + "3 2 1:2 / /run/pot-test2 rw - ext4 device rw\n")
    with pytest.raises(helper.PreparationError, match="runtime_not_tmpfs"):
        helper.require_tmpfs(Path("/run/pot-test2"), mountinfo)


def test_atomic_publish_is_private_and_failed_replace_preserves_previous_file(
    tmp_path, monkeypatch
):
    uid, gid = os.getuid(), os.getgid()
    metadata = SimpleNamespace(st_mode=stat.S_IFDIR | 0o750, st_uid=0, st_gid=gid)
    helper.directory_metadata(metadata, gid)
    metadata.st_mode = stat.S_IFDIR | 0o770
    with pytest.raises(helper.PreparationError):
        helper.directory_metadata(metadata, gid)
    # Do not perform privileged ownership changes in this unit test.
    ownership = []
    monkeypatch.setattr(
        helper.os, "fchown", lambda fd, owner, group: ownership.append((owner, group))
    )
    directory = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        helper.atomic_write(directory, b"SYNTHETIC=first\n", uid, gid)
        output = tmp_path / "secrets.env"
        assert output.read_bytes() == b"SYNTHETIC=first\n"
        assert stat.S_IMODE(output.stat().st_mode) == 0o600 and ownership == [(uid, gid)]

        def fail_replace(*args, **kwargs):
            raise OSError("Synthetic replace failure")

        monkeypatch.setattr(helper.os, "replace", fail_replace)
        with pytest.raises(OSError):
            helper.atomic_write(directory, b"SYNTHETIC=second\n", uid, gid)
        assert output.read_bytes() == b"SYNTHETIC=first\n"
        assert sorted(path.name for path in tmp_path.iterdir()) == ["secrets.env"]
    finally:
        os.close(directory)
