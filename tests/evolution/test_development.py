from __future__ import annotations

import json
import os

import pytest

from groupagent.domain import DomainError
from groupagent.evolution import DevelopmentManifest
from groupagent.evolution.__main__ import _activation_report
from groupagent.evolution.development import (
    EvaluationSuite,
    Snapshot,
    evaluate_code,
    write_source_archive,
)
from groupagent.evolution.models import CodeBundle, CodeProposal, canonical, digest

PATH = "src/demolab/calculator.py"
CONFIG = "src/demolab/config.py"
OLD = "def score(value):\n    return value + 1\n"
NEW = "def score(value):\n    return value * 2\n"


@pytest.fixture
def manifest(tmp_path):
    root = tmp_path.resolve()
    snapshot = root / "source/src/demolab"
    snapshot.mkdir(parents=True)
    (snapshot / "calculator.py").write_text(OLD)
    (snapshot / "config.py").write_text("# synthetic approved source, read only")
    path = root / "manifest.json"
    path.write_text(
        json.dumps(
            {
                "name": "calculator",
                "files": {
                    PATH: digest(OLD.encode()),
                    CONFIG: digest((snapshot / "config.py").read_bytes()),
                },
                "editable": [PATH],
            }
        )
    )
    return DevelopmentManifest.load(path)


def proposal(manifest, text=NEW, path=PATH, before=None):
    return CodeProposal.model_validate(
        {
            "name": "improve-score",
            "problem": "Return the doubled score.",
            "baseline_sha256": manifest.sha256,
            "edits": [
                {"path": path, "text": text, "before_sha256": before or digest(OLD.encode())}
            ],
        }
    )


@pytest.mark.parametrize(
    "path", [CONFIG, "src/../../tests/suite.py", "/tmp/test.py", "tests/test_code.py"]
)
def test_path_outside_editable_snapshot_rejected(manifest, path):
    with pytest.raises(DomainError, match="approved source snapshot"):
        manifest.candidate(proposal(manifest, path=path))
    with pytest.raises(ValueError):
        Snapshot(name="unsafe", files={path: "a" * 64}, editable=(path,))


def test_preimage_and_snapshot_mutation_rejected(manifest):
    assert manifest.inspect(PATH) == (digest(OLD.encode()), OLD)
    with pytest.raises(DomainError, match="preimage digest"):
        manifest.candidate(proposal(manifest, before="b" * 64))
    with pytest.raises(DomainError, match="change the approved source"):
        manifest.candidate(proposal(manifest, text=OLD))
    (manifest.root / CONFIG).write_text("# tampered before evaluation")
    with pytest.raises(ValueError, match="recorded digest"):
        manifest.candidate(proposal(manifest))


def test_symlink_cannot_supply_matching_preimage(manifest, tmp_path):
    path = manifest.root / PATH
    path.unlink()
    outside = tmp_path / "outside.py"
    outside.write_text(OLD)
    path.symlink_to(outside)
    with pytest.raises(DomainError, match="snapshot is unavailable"):
        manifest.inspect(PATH)


@pytest.mark.sandbox
@pytest.mark.parametrize("tampered", [False, True])
async def test_independent_isolated_development_evaluation(manifest, tmp_path, tampered):
    image = os.environ.get("GROUPAGENT_WORKER_IMAGE")
    if not image:
        pytest.skip("Set GROUPAGENT_WORKER_IMAGE to a pinned local worker")
    source = (
        NEW
        if not tampered
        else (
            "from pathlib import Path\n"
            "def score(value):\n"
            "    Path('/work/report.json').write_text('{\"passed\":true}')\n"
            "    return {'passed': True}\n"
        )
    )
    candidate = proposal(manifest, text=source)
    bundle = CodeBundle(
        version_id="a" * 32,
        body_sha256=digest(canonical(candidate.model_dump(mode="json"))),
        proposal=candidate,
    )
    suite = EvaluationSuite.model_validate(
        {
            "module": "demolab.calculator",
            "function": "score",
            "cases": [
                {"name": "twenty", "argument": 20, "expected": 40, "baseline_expected": 21},
                {"name": "negative", "argument": -3, "expected": -6, "baseline_expected": -2},
            ],
        }
    )
    report, files, diff = await evaluate_code(
        manifest, bundle, suite, image=image, docker_host=os.environ.get("GROUPAGENT_DOCKER_HOST")
    )
    assert report.passed is (not tampered)
    assert report.checks["baseline-compile"] and report.checks["candidate-compile"]
    assert report.checks["candidate-0"] is (not tampered)
    assert PATH in diff and files[CONFIG] == (manifest.root / CONFIG).read_bytes()
    assert (manifest.root / PATH).read_text() == OLD

    (tmp_path / "report.json").write_text(report.model_dump_json())
    archive = tmp_path / "candidate.zip"
    write_source_archive(archive, files)
    assert _activation_report(tmp_path) == report
    archive.unlink()
    write_source_archive(archive, {**files, PATH: b"# changed after evaluation"})
    with pytest.raises(ValueError, match="does not match"):
        _activation_report(tmp_path)
