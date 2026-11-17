"""Read real timezone candidates without committing a schedule or work."""

from datetime import UTC, datetime

import pytest

from groupagent.domain import WorkState

from .test_react_turn import ModelScript, call, finish, offered_properties, tool_receipt
from .test_temporal_capability import ingest

pytestmark = pytest.mark.postgres


@pytest.mark.parametrize(
    ("date", "local_time", "zone", "status", "candidates"),
    [
        (
            "2026-11-01",
            "01:30",
            "America/Los_Angeles",
            "ambiguous",
            [
                {
                    "fold": 0,
                    "local": "2026-11-01T01:30:00-07:00",
                    "utc": "2026-11-01T08:30:00+00:00",
                },
                {
                    "fold": 1,
                    "local": "2026-11-01T01:30:00-08:00",
                    "utc": "2026-11-01T09:30:00+00:00",
                },
            ],
        ),
        ("2026-03-08", "02:30", "America/Los_Angeles", "nonexistent", []),
        (
            "2026-11-01",
            "01:30",
            None,
            "unique",
            [{"fold": 0, "local": "2026-11-01T01:30:00+08:00", "utc": "2026-10-31T17:30:00+00:00"}],
        ),
    ],
)
async def test_clock_previews_local_and_utc_options_before_any_write(
    conversation_env, date, local_time, zone, status, candidates
):
    env = conversation_env
    now = datetime(2026, 3, 1, tzinfo=UTC)
    env.coordinator.clock = lambda: now
    event = ingest(env, "Pot，帮我核对一下这个指定的本地时间。", now, now)

    def preview(request):
        schema = offered_properties(request, "read_group_context")["wall_time"]
        assert set(schema["properties"]) == {"date", "local_time", "time_zone"}
        wall_time = {"date": date, "local_time": local_time}
        if zone is not None:
            wall_time["time_zone"] = zone
        return call("read_group_context", "wall-preview", resource="clock", wall_time=wall_time)

    def answer(request):
        receipt = tool_receipt(request)
        assert receipt["status"] == "observed" and not receipt["committed"]
        assert receipt["facts"]["now_utc"] == now.isoformat()
        observed = receipt["facts"]["wall_time"]
        assert observed["time_zone"] == (zone or "Asia/Shanghai")
        assert observed["status"] == status and observed["candidates"] == candidates
        assert observed["requires_author_choice"] == (status != "unique")
        if status == "ambiguous":
            assert receipt["code"] == "ambiguous_dst_fold"
            return finish(
                "你指的是 UTC−07:00 的第一次，还是 UTC−08:00 的第二次？", needs_input=True
            )
        if status == "nonexistent":
            assert receipt["code"] == "nonexistent_local_time"
            return finish("当地时钟会跳过 02:30。你希望换到几点？", needs_input=True)
        assert "code" not in receipt
        return finish("这个时间对应北京时间 01:30。")

    model = ModelScript(preview, answer)
    env.models[event.id] = model
    result = await env.coordinator.process(event)
    assert result.decision.kind == ("reply" if status == "unique" else "clarify")
    assert result.delivery_id and not result.work_id and len(model.requests) == 2
    assert env.coordinator.scheduler.list(env.actor) == []
    with env.store.transaction(env.actor) as tx:
        assert not tx.list_work()
        assert len(tx.list_delivery()) == 1


async def test_ambiguous_wall_preview_keeps_independent_work_admissible(conversation_env):
    env = conversation_env
    now = datetime(2026, 10, 30, tzinfo=UTC)
    env.coordinator.clock = lambda: now
    event = ingest(
        env,
        "Pot，洛杉矶 11 月 1 日若有两个 01:30，先问我选哪个。另请实际运行 Python 算 21 乘 3。",
        now,
        now,
    )

    def queue(request):
        assert tool_receipt(request)["facts"]["wall_time"]["requires_author_choice"]
        return call(
            "start_work",
            "independent-compute",
            goal="实际运行 Python 算 21 乘 3",
            work_capabilities=["compute"],
            required_work_tools=["python_compute"],
            outputs=[{"format": "text"}],
            finish_on_commit=True,
        )

    env.models[event.id] = ModelScript(
        call(
            "read_group_context",
            "clock-preview",
            resource="clock",
            wall_time={
                "date": "2026-11-01",
                "local_time": "01:30",
                "time_zone": "America/Los_Angeles",
            },
        ),
        queue,
    )
    result = await env.coordinator.process(event)
    assert result.work_id and result.delivery_id
    with env.store.transaction(env.actor) as tx:
        (work,) = tx.list_work()
        assert work.id == result.work_id and work.state == WorkState.QUEUED
        assert work.goal == "实际运行 Python 算 21 乘 3"
    assert env.coordinator.scheduler.list(env.actor) == []
