"""Synthetic native metadata contracts; no WeChat account or network access."""

from __future__ import annotations

import httpx
import pytest

from groupagent.domain import DomainError

from .conftest import ACCOUNT, GROUP, ROOM


def operation(state="queued", **changes):
    return {
        "id": "synthetic-metadata-operation",
        "accountId": ACCOUNT,
        "kind": "metadata",
        "state": state,
        "result": {},
        "createdAt": "2026-11-16T08:00:00Z",
        "updatedAt": "2026-11-16T08:00:00Z",
        **changes,
    }


def completed(**changes):
    return operation(
        "succeeded",
        **{
            "result": {"memberCount": 2, "profileProjection": "complete", "completeRoster": True},
            **changes,
        },
    )


def metadata_responses(daemon, *polls, submission=None):
    pending = iter(polls)

    def respond(request):
        if request.url.path.endswith("/members/refresh"):
            assert request.method == "POST" and not request.content
            assert "idempotency-key" not in request.headers
            return httpx.Response(202, json=submission or operation())
        if "/operations/" in request.url.path:
            assert request.method == "GET"
            return httpx.Response(200, json=next(pending))
        return None

    daemon.failure = respond


async def test_targeted_refresh_waits_for_completion_then_reads_cached_room_name(adapter, daemon):
    metadata_responses(daemon, operation("running"), completed())
    daemon.roster = [
        {
            "id": "stable-member",
            "username": "stable-member",
            "roomDisplayName": "新群昵称",
            "displayName": "Private remark",
        },
        {
            "id": "unknown-name",
            "username": "unknown-name",
            "roomDisplayName": None,
            "displayName": "Private nickname",
        },
    ]
    receipt = await adapter.refresh_members(GROUP, poll_interval_seconds=0.001)
    assert receipt.state == "succeeded" and receipt.source == "remote_refresh"
    assert receipt.group_id == GROUP and receipt.complete_roster and receipt.member_count == 2
    page = await adapter.members()
    assert page.members[0].room_display_name == "新群昵称"
    assert page.members[0].member_id == "stable-member"
    assert page.members[1].room_display_name is None
    assert all(member.coverage == "cached" for member in page.members)
    assert "Private" not in page.model_dump_json()
    posts = [request for request in daemon.requests if request.method == "POST"]
    assert len(posts) == 1 and posts[0].url.path.endswith(f"groups/{ROOM}/members/refresh")
    assert not any("contact-sync" in request.url.path for request in daemon.requests)


@pytest.mark.parametrize(
    "response,code",
    [
        (operation(accountId="another-account"), "scope_denied"),
        (operation(id="another-operation"), "scope_denied"),
        (operation(kind="group_mutation"), "scope_denied"),
        (operation("unknown"), "invalid_channel_response"),
        (
            completed(result={"memberCount": 2, "profileProjection": "partial"}),
            "invalid_channel_response",
        ),
        (
            completed(
                result={
                    "memberCount": True,
                    "profileProjection": "complete",
                    "completeRoster": True,
                }
            ),
            "invalid_channel_response",
        ),
        (operation("failed", result={"code": "operation_failed"}), "roster_refresh_failed"),
        (operation("failed", result={"code": "auth_required"}), "auth_required"),
        (operation("cancelled"), "roster_refresh_failed"),
    ],
)
async def test_failed_or_inconsistent_operation_never_reports_fresh_roster(
    adapter, daemon, response, code
):
    metadata_responses(daemon, response)
    with pytest.raises(DomainError) as error:
        await adapter.refresh_members(GROUP, poll_interval_seconds=0.001)
    assert error.value.code == code
    assert sum(request.method == "POST" for request in daemon.requests) == 1


async def test_refresh_timeout_keeps_operation_identity_without_retrying_submission(
    adapter, daemon
):
    def respond(request):
        if request.url.path.endswith("/members/refresh") or "/operations/" in request.url.path:
            return httpx.Response(202, json=operation("running"))
        return None

    daemon.failure = respond
    with pytest.raises(DomainError) as error:
        await adapter.refresh_members(GROUP, timeout_seconds=0.01, poll_interval_seconds=0.001)
    assert error.value.code == "roster_refresh_timeout"
    assert error.value.resource_ref == "synthetic-metadata-operation"
    assert sum(request.method == "POST" for request in daemon.requests) == 1
    assert not any(request.url.path.endswith("/members") for request in daemon.requests)
    metadata_responses(daemon, completed())
    receipt = await adapter.refresh_members(GROUP, poll_interval_seconds=0.001)
    assert receipt.reused and receipt.operation_id == error.value.resource_ref
    assert sum(request.method == "POST" for request in daemon.requests) == 1


async def test_expired_unresolved_refresh_is_reconciled_without_another_submission(adapter, daemon):
    adapter._pending_roster_refresh = ("synthetic-metadata-operation", -1_000_000)
    metadata_responses(daemon, operation("running"))
    with pytest.raises(DomainError) as error:
        await adapter.refresh_members(GROUP)
    assert error.value.code == "roster_refresh_pending"
    assert error.value.resource_ref == "synthetic-metadata-operation"
    assert not any(request.method == "POST" for request in daemon.requests)


async def test_lost_refresh_submission_is_metadata_uncertainty_and_does_not_retry(adapter, daemon):
    def respond(request):
        if request.url.path.endswith("/members/refresh"):
            raise httpx.ReadTimeout("synthetic lost metadata response", request=request)
        return None

    daemon.failure = respond
    with pytest.raises(DomainError) as error:
        await adapter.refresh_members(GROUP)
    assert error.value.code == "roster_refresh_unknown"
    assert not error.value.retryable and error.value.effect_state == "unknown"
    assert sum(request.method == "POST" for request in daemon.requests) == 1


async def test_foreign_group_refresh_is_rejected_before_requests(adapter, daemon):
    with pytest.raises(DomainError) as error:
        await adapter.refresh_members("another-group")
    assert error.value.code == "scope_denied" and not daemon.requests


async def test_group_updated_preserves_only_target_room_invalidation_cursor(adapter, daemon):
    daemon.events = [
        {
            "cursor": str(index),
            "kind": "group.updated",
            "data": {
                "accountId": ACCOUNT,
                "groupId": room,
                "untrustedPayload": "do-not-project",
            },
            "createdAt": "2026-11-16T08:00:00Z",
        }
        for index, room in enumerate(("another@chatroom", ROOM), start=1)
    ]
    daemon.next_cursor = "2"
    batch = await adapter.consume()
    assert batch.events == () and [cursor.value for cursor in batch.roster_updates] == ["2"]
    assert batch.roster_updates[0].group_id == GROUP
    assert [item.reason for item in batch.skipped] == ["outside_group", "control_event"]
    assert "do-not-project" not in batch.model_dump_json()
    assert not any(request.method == "POST" for request in daemon.requests)
