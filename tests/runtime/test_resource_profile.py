"""Composition and durable intake under the opt-in small-host profile."""

from types import SimpleNamespace

import pytest

from groupagent.config import AppConfig
from groupagent.coordinator import Decision
from groupagent.domain import EventKind
from groupagent.resources import MemorySnapshot, ResourceGovernor, ResourcePolicy
from groupagent.runtime.app import channel_config
from groupagent.runtime.resource_profile import worker_limits, worker_tool_names
from groupagent.runtime.toolkit import WorkToolkit

from .test_app import app_for, event


def small_config():
    return AppConfig.model_validate(
        {
            "group": {
                "internal_id": "synthetic",
                "external_group_id": "synthetic@chatroom",
                "owner_ids": ["owner"],
            },
            "resources": {"profile": "server_2gb"},
        }
    )


def test_worker_contract_bounds_all_transient_processes():
    limits = worker_limits(small_config())
    assert (limits.memory_mb, limits.cpus, limits.cpu_shares, limits.oom_score_adj) == (
        512,
        0.75,
        128,
        500,
    )
    assert (limits.workspace_mb, limits.temp_mb, limits.pids) == (128, 64, 192)
    assert limits.max_input_bytes == 32 * 1024**2
    assert limits.max_file_bytes == 25 * 1024**2
    assert limits.max_artifact_bytes == 26 * 1024**2
    assert channel_config(small_config()).max_attachment_bytes == 25 * 1024**2


def test_heavy_tool_admission_leaves_conversation_and_control_lane_free():
    names = frozenset(
        {
            "document_generate",
            "browser_open",
            "speech_synthesize",
            "python_compute",
            "artifact_import",
            "provider_ocr",
            "artifact_from_workspace",
            "artifact_deliver",
            "team_list",
            "request_input",
            "workspace_read_text",
            "web_search",
        }
    )
    assert worker_tool_names(names) == names - {
        "artifact_deliver",
        "team_list",
        "request_input",
        "workspace_read_text",
        "web_search",
    }


def test_release_attempt_drops_only_the_quiescent_attempt_cache():
    toolkit = object.__new__(WorkToolkit)
    toolkit._services = {"finished": (None, None, None), "active": (None, None, None)}
    toolkit.release_attempt("work", "finished")
    toolkit.release_attempt("work", "finished")
    assert list(toolkit._services) == ["active"]


@pytest.mark.postgres
async def test_audio_resource_wait_does_not_burn_retries_or_block_later_text(runtime_env):
    env = runtime_env
    audio = event(env, "", kind=EventKind.AUDIO)
    text = event(env, "Pot，先回答这个问题")
    app, _ = app_for(
        env,
        {
            text.id: Decision(
                kind="reply", response="我在。", reason_code="synthetic", confidence=1
            ),
        },
    )
    config = AppConfig.model_validate(
        {
            **app.config.model_dump(exclude={"runtime", "limits", "resources"}),
            "resources": {"profile": "server_2gb"},
        }
    )
    app.config = app.coordinator.config = config
    app.inputs = SimpleNamespace(preprocess_audio=None)  # Must never be invoked without capacity.
    app.worker_governor = ResourceGovernor(
        ResourcePolicy(enabled=True),
        sampler=lambda: MemorySnapshot(0),
    )
    env.channel.append_incoming(audio)
    env.channel.append_incoming(text)
    await app.poll_intake()
    for _ in range(4):
        await app._process(audio)
    with env.store.transaction(env.actor) as tx:
        row = tx.conn.execute(
            "SELECT attempts,blocked,code FROM ga_event_retry WHERE group_id=%s AND event_id=%s",
            (tx.group_id, audio.id),
        ).fetchone()
        assert row == {"attempts": 0, "blocked": False, "code": "resource_deferred"}
        assert tx.get_decision(audio.id) is None
    assert await app.process_pending(batched=False) == 1
    with env.store.transaction(env.actor) as tx:
        assert tx.get_decision(text.id) is not None
        assert tx.get_decision(audio.id) is None
    await app.close()


@pytest.mark.postgres
async def test_heavy_work_defers_optional_maintenance(runtime_env):
    app, _ = app_for(runtime_env, {})
    app.config = app.coordinator.config = AppConfig.model_validate(
        {
            **app.config.model_dump(exclude={"runtime", "limits", "resources"}),
            "resources": {"profile": "server_2gb"},
        }
    )
    app._processing.add("synthetic-busy-event")
    await app.maintenance(force=True)
    assert app._last_maintenance > 0
    await app.close()


@pytest.mark.postgres
async def test_failed_worker_startup_keeps_chat_available(runtime_env):
    from groupagent.domain import DomainError

    env = runtime_env
    request = event(env, "Pot，你在吗")
    app, _ = app_for(
        env,
        {
            request.id: Decision(
                kind="reply", response="我在。", reason_code="synthetic", confidence=1
            ),
        },
    )
    started = []

    async def reconcile():
        started.append(True)
        raise DomainError("resource_reconciliation_required", "Synthetic unavailable inventory")

    app.worker_startup = reconcile
    await app.start()
    env.channel.append_incoming(request)
    await app.step()
    assert started == [True]
    with env.store.transaction(env.actor) as tx:
        assert tx.get_decision(request.id) is not None
    await app.close()
