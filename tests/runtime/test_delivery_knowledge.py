"""Native queue knowledge is distinct from uncertain writes and acceptance."""

import pytest

from groupagent.domain import DeliveryState
from groupagent.persistence.delivery_knowledge import projected_delivery_state

pytestmark = pytest.mark.public_image_workflow


@pytest.mark.parametrize(
    "parts,expected",
    [
        ([], "unknown"),
        ([{"state": "pending"}], "unknown"),
        ([{"state": "accepted"}], "unknown"),
        ([{"state": "accepted", "platform_message_id": "native-message"}], "dispatching"),
        ([{"state": "accepted", "platform_message_id": " "}], "unknown"),
        ([{"state": "submitted", "platform_job_id": "native-job"}], "dispatching"),
        (
            [
                {"state": "accepted", "platform_message_id": "native-message"},
                {"state": "submitted", "platform_job_id": "native-send"},
                {"state": "pending"},
            ],
            "dispatching",
        ),
        (
            [
                {"state": "accepted"},
                {"state": "submitted", "platform_job_id": "native-job"},
            ],
            "unknown",
        ),
        (
            [
                {"state": "submitted", "platform_job_id": "native-one"},
                {"state": "submitted", "platform_job_id": "native-two"},
            ],
            "dispatching",
        ),
        ([{"state": "submitted"}], "unknown"),
        ([{"state": "submitted", "platform_job_id": ""}], "unknown"),
        ([{"state": "submitted", "platform_job_id": "  "}], "unknown"),
        ([{"state": "submitted", "platform_job_id": 42}], "unknown"),
        ([{"state": "unknown", "platform_job_id": "native-review-job"}], "unknown"),
        ([{"state": "needs_review", "platform_job_id": "native-review-job"}], "unknown"),
        (
            [
                {"state": "submitted", "platform_job_id": "native-job"},
                {"state": "submitted"},
            ],
            "unknown",
        ),
        (
            [
                {"state": "submitted", "platform_job_id": "native-job"},
                {"state": "unknown"},
            ],
            "unknown",
        ),
        (
            [
                {"state": "submitted", "platform_job_id": "native-job"},
                {"state": "rejected"},
            ],
            "unknown",
        ),
    ],
)
def test_only_fully_accounted_native_jobs_project_to_dispatching(parts, expected):
    assert projected_delivery_state(DeliveryState.UNKNOWN, parts) == expected


@pytest.mark.parametrize("state", ["pending", "dispatching", "accepted", "rejected"])
def test_projection_does_not_invent_another_parent_outcome(state):
    assert projected_delivery_state(state, [{"state": "unknown"}]) == state
