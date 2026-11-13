from __future__ import annotations

import pytest

from groupagent.artifacts import (
    ArtifactScope,
    ArtifactStore,
    Deck,
    DocumentService,
    Report,
    Section,
    Slide,
    TableData,
)


@pytest.fixture
def scope():
    return ArtifactScope("synthetic-group", "synthetic-work", "synthetic-author")


@pytest.fixture
def store(tmp_path):
    with ArtifactStore(tmp_path / "artifacts") as store:
        yield store


@pytest.fixture
def service(store):
    return DocumentService(store)


@pytest.fixture
def table():
    return TableData(("Measure", "Result"), (("Sample count", "24"), ("Accuracy", "87 percent")))


@pytest.fixture
def report(table):
    return Report(
        title="Course Project Report",
        subtitle="A classroom comparison of two study methods",
        sections=(
            Section(
                "Purpose", ("We compared two study methods using a small classroom exercise.",)
            ),
            Section(
                "Findings",
                (
                    "Students completed the same quiz after a week of practice.",
                    "The result supports a larger follow up study.",
                ),
                (table,),
            ),
            Section("Next Steps", ("Repeat the exercise with a larger sample next term.",)),
        ),
    )


@pytest.fixture
def deck(table):
    return Deck(
        "Course Project",
        (
            Slide(
                "Study Design", ("Compare two study methods.", "Use the same quiz for both groups.")
            ),
            Slide("Findings", ("The classroom exercise included 24 students.",), table),
            Slide("Next Steps", ("Repeat the exercise with a larger sample next term.",)),
        ),
    )
