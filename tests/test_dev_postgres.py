import os
import runpy
from pathlib import Path

import pytest

HELPER = runpy.run_path(str(Path(__file__).resolve().parents[1] / "scripts/dev_postgres.py"))


@pytest.mark.parametrize("child", ["db", "socket", "dsn.txt", "server.log", "cluster-owner.json"])
def test_developer_helper_rejects_child_symlinks_without_modifying_targets(tmp_path, child):
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    target = tmp_path / "unrelated"
    target.write_text("untouched")
    (runtime / child).symlink_to(target)
    with pytest.raises(ValueError, match="symlink"):
        HELPER["validate_paths"](runtime)
    assert target.read_text() == "untouched"


def test_existing_unmarked_database_cannot_be_stopped(tmp_path):
    (tmp_path / "db").mkdir()
    (tmp_path / "db/PG_VERSION").write_text("16")
    with pytest.raises(ValueError, match="not registered"):
        HELPER["require_owned_cluster"](tmp_path, 55439)


def test_hard_link_cannot_truncate_another_file(tmp_path):
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    target = tmp_path / "unrelated"
    target.write_text("untouched")
    output = runtime / "dsn.txt"
    os.link(target, output)
    with pytest.raises(ValueError, match="one link"):
        HELPER["validate_paths"](runtime)
    with pytest.raises(ValueError, match="one link"):
        HELPER["write_private_text"](output, "replacement")
    assert target.read_text() == "untouched"


def test_owned_single_link_output_is_replaced_privately(tmp_path):
    output = tmp_path / "dsn.txt"
    output.write_text("previous longer content")
    HELPER["write_private_text"](output, "new\n")
    assert output.read_text() == "new\n"
    assert output.stat().st_mode & 0o777 == 0o600
