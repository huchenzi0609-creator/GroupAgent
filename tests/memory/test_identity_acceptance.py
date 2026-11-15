"""Fifty frozen synthetic identity/provenance cases against actual PostgreSQL.

The fixture supplies explicit facts/actions; it does not score natural-language
extraction or a live summarizer. Each case owns a separate group via conftest.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass

import pytest

from groupagent.domain import DomainError, EventKind, TeamTaskState
from groupagent.memory import (
    DeterministicFixtureSummarizer,
    MemoryService,
    RosterMember,
    RosterSnapshot,
)

pytestmark = pytest.mark.postgres


@dataclass(frozen=True)
class Context:
    id: str
    name_a: str
    name_b: str
    renamed: str
    alias: str
    task_a: str
    task_b: str
    corrected: str
    progress: str


CONTEXTS = (
    Context(
        "course",
        "小林",
        "小许",
        "林同学",
        "课程助教",
        "整理课程参考书目录",
        "核对课堂展示页码",
        "整理新版参考书目录",
        "目录前两章已核对",
    ),
    Context(
        "lab",
        "阿宁",
        "阿衡",
        "宁实验员",
        "值日协调员",
        "登记试剂库存数量",
        "校准温度计读数",
        "登记冷藏试剂库存",
        "已录入第一批试剂",
    ),
    Context(
        "project",
        "陈老师",
        "李老师",
        "陈研究员",
        "项目联系人",
        "整理访谈问题清单",
        "核对调查表选项",
        "整理家长访谈问题",
        "完成开放问题初稿",
    ),
    Context(
        "volunteer",
        "小周",
        "小吴",
        "周志愿者",
        "活动负责人",
        "检查活动签到名单",
        "联系场地管理员",
        "检查下午场签到名单",
        "已核对预约编号",
    ),
    Context(
        "workshop",
        "小乔",
        "小唐",
        "乔讲师",
        "工作坊联络人",
        "准备纸桥实验材料",
        "统计讲义领取数量",
        "准备加固纸桥材料",
        "已分装三组材料",
    ),
)

SCENARIOS = (
    "duplicate_names",
    "rename_swap",
    "nickname_priority",
    "two_speakers_i",
    "own_correction",
    "foreign_correction_denied",
    "recall_progress",
    "recall_then_replay",
    "compact_restart",
    "private_topic_compaction",
)
CASES = tuple((scenario, context) for scenario in SCENARIOS for context in CONTEXTS)
CASE_IDS = tuple(f"IM-{scenario}-{context.id}" for scenario, context in CASES)
CASE_SHA256 = "503c4042ad0ce25d4c0cf34cce7927ed4c73150bb929809769e3fabc9b688a6b"


class ExplicitSyntheticRoster:
    def __init__(self, group_id, entries, *, partial=False):
        self.snapshot = RosterSnapshot(
            members=[
                RosterMember(
                    group_id=group_id,
                    member_id=member_id,
                    room_display_name=name,
                    membership_state="present",
                    coverage="verified",
                    name_source="roomDisplayName" if name else "unknown",
                )
                for member_id, name in entries
            ],
            partial=partial,
            source_generation="synthetic-identity-acceptance-v1",
        )

    async def fetch(self, group_id):
        assert all(member.group_id == group_id for member in self.snapshot.members)
        return self.snapshot


def test_identity_acceptance_manifest_is_frozen():
    body = [
        {"id": identifier, "scenario": scenario, "context": asdict(context)}
        for identifier, (scenario, context) in zip(CASE_IDS, CASES, strict=True)
    ]
    canonical = json.dumps(body, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    assert len(body) == len(set(CASE_IDS)) == 50
    assert hashlib.sha256(canonical.encode()).hexdigest() == CASE_SHA256


@pytest.mark.parametrize("scenario,case", CASES, ids=CASE_IDS)
async def test_identity_acceptance_group(
    store, memory, members, actor, owner, maintenance, emit, scenario, case
):
    other = actor.model_copy(update={"member_id": "synthetic-member-b"})
    duplicate = scenario in {"duplicate_names", "rename_swap", "two_speakers_i"}
    name_b = case.name_a if duplicate else case.name_b
    await members.sync_startup(
        owner,
        "initial",
        ExplicitSyntheticRoster(
            actor.group_id, [(actor.member_id, case.name_a), (other.member_id, name_b)]
        ),
    )
    first = emit(actor, f"我负责{case.task_a}，请登记为我的任务。")
    second = emit(
        other,
        f"我负责{case.task_b}，前一条不是我的任务。",
        kind=EventKind.QUOTE if scenario == "two_speakers_i" else EventKind.TEXT,
        reply_to=first.id if scenario == "two_speakers_i" else None,
    )
    tasks = []
    facts = []
    for person, source, title in ((actor, first, case.task_a), (other, second, case.task_b)):
        with store.transaction(person) as tx:
            tasks.append(tx.create_team_task(title, source_event_id=source.id))
        facts.append(
            memory.record_fact(
                person, "responsibility", title, source_event_ids=[source.id], explicit=True
            )
        )

    if scenario == "duplicate_names":
        with pytest.raises(DomainError) as caught:
            members.resolve_unique(actor, case.name_a)
        assert caught.value.code == "ambiguous_reference"
        assert {item.member_id for item in members.resolve_name(actor, case.name_a)} == {
            actor.member_id,
            other.member_id,
        }

    elif scenario == "rename_swap":
        await members.refresh(
            owner,
            "rename-first",
            ExplicitSyntheticRoster(
                actor.group_id, [(actor.member_id, case.renamed)], partial=True
            ),
        )
        assert members.resolve_unique(actor, case.name_a).member_id == other.member_id
        assert members.resolve_unique(actor, case.renamed).member_id == actor.member_id
        await members.refresh(
            owner,
            "swap-names",
            ExplicitSyntheticRoster(
                actor.group_id,
                [(actor.member_id, case.name_a), (other.member_id, case.renamed)],
            ),
        )
        assert members.resolve_unique(actor, case.renamed).member_id == other.member_id

    elif scenario == "nickname_priority":
        for person in (actor, other):
            known = members.get(owner, person.member_id)
            named = members.confirm_name(
                owner, person.member_id, case.alias, expected_revision=known.revision
            )
            assert named.name_source == "room_display_name"
        assert members.resolve_name(actor, case.alias) == []
        await members.refresh(
            owner,
            "confirmed-exception",
            ExplicitSyntheticRoster(
                actor.group_id, [(actor.member_id, None), (other.member_id, case.name_b)]
            ),
        )
        assert members.resolve_unique(actor, case.alias).member_id == actor.member_id
        assert members.resolve_unique(actor, case.name_b).member_id == other.member_id

    elif scenario == "two_speakers_i":
        hits = {hit.id: hit for hit in memory.retrieve(actor, include_tasks=False)}
        for source, person in ((first, actor), (second, other)):
            assert hits[source.id].author_id == person.member_id
            assert {ref.actor_id for ref in hits[source.id].source_refs} == {person.member_id}
        with pytest.raises(DomainError) as caught:
            memory.record_fact(
                actor,
                "responsibility",
                "不得冒用另一个人的我",
                subject_id=other.member_id,
                source_event_ids=[first.id],
                explicit=True,
            )
        assert caught.value.code == "permission_denied"

    elif scenario == "own_correction":
        summary = await memory.compact(actor, force=True)
        assert summary is not None
        change = emit(actor, f"更正我自己的分工：{case.corrected}；另一人的任务不变。")
        facts[0] = memory.correct_fact(
            actor,
            facts[0].id,
            case.corrected,
            expected_revision=1,
            source_event_ids=[change.id],
            explicit=True,
        )
        with store.transaction(actor) as tx:
            tasks[0] = tx.update_team_task(
                tasks[0].id,
                expected_revision=1,
                title=case.corrected,
                source_event_id=change.id,
            )
        assert facts[0].revision == 2 and facts[0].value == case.corrected
        assert all(hit.id != summary.id for hit in memory.retrieve(actor, include_tasks=False))

    elif scenario == "foreign_correction_denied":
        change = emit(other, f"我想把另一个人的任务改成{case.corrected}。")
        with pytest.raises(DomainError) as caught:
            memory.correct_fact(
                other,
                facts[0].id,
                case.corrected,
                expected_revision=1,
                source_event_ids=[change.id],
                explicit=True,
            )
        assert caught.value.code == "permission_denied"
        with pytest.raises(DomainError) as caught:
            with store.transaction(other) as tx:
                tx.update_team_task(
                    tasks[0].id,
                    expected_revision=1,
                    progress=case.progress,
                    source_event_id=change.id,
                )
        assert caught.value.code == "permission_denied"
        assert memory.get_fact(actor, facts[0].id).revision == 1

    elif scenario == "recall_progress":
        change = emit(actor, f"我的进度：{case.progress}。")
        with store.transaction(actor) as tx:
            tasks[0] = tx.update_team_task(
                tasks[0].id,
                expected_revision=1,
                progress=case.progress,
                source_event_id=change.id,
            )
        progress = memory.record_fact(
            actor, "progress", case.progress, source_event_ids=[change.id], explicit=True
        )
        recall = emit(
            maintenance,
            kind=EventKind.RECALL,
            reply_to=change.id,
            trusted_actor_origin="agentwechat_authenticated_recall_metadata",
        )
        memory.recall(maintenance, recall.id)
        with store.transaction(actor) as tx:
            current = tx.get_team_task(tasks[0].id)
            assert current.progress == "" and current.content_state == "active"
            assert change.id not in current.source_event_ids
        with pytest.raises(DomainError) as caught:
            memory.get_fact(actor, progress.id)
        assert caught.value.code == "source_unavailable"
        assert memory.retrieve(actor, case.progress) == []

    elif scenario == "recall_then_replay":
        recall = emit(
            maintenance,
            kind=EventKind.RECALL,
            reply_to=first.id,
            trusted_actor_origin="agentwechat_authenticated_recall_metadata",
        )
        memory.recall(maintenance, recall.id)
        with store.transaction(actor) as tx:
            replay = tx.ingest_event(first, "identity-acceptance-replay", "1", 0)
            assert not replay.inserted and replay.event.content_state == "recalled"
        with pytest.raises(DomainError) as caught:
            memory.get_fact(actor, facts[0].id)
        assert caught.value.code == "source_unavailable"
        assert memory.retrieve(actor, case.task_a) == []

    elif scenario == "compact_restart":
        summary = await memory.compact(actor, force=True)
        assert summary is not None
        assert summary.provider == "deterministic-fixture" and summary.incomplete
        assert summary.source_event_ids == [first.id, second.id]
        assert f"{actor.member_id}:" in summary.text and f"{other.member_id}:" in summary.text
        memory = MemoryService(store, DeterministicFixtureSummarizer())
        assert memory.checkpoint(actor)["through_sequence"] == summary.last_sequence
        hits = memory.retrieve(actor, case.task_a, include_tasks=False)
        assert any(hit.id == summary.id and hit.incomplete for hit in hits)
        assert any(hit.id == first.id and hit.author_id == actor.member_id for hit in hits)
        assert await memory.compact(actor, force=True) is None

    elif scenario == "private_topic_compaction":
        secret = emit(actor, f"仅限本人话题：{case.corrected}")
        topic = memory.create_topic(actor, case.id + "-private", visibility="participants")
        memory.link_event(actor, secret.id, topic.id, evidence_event_ids=[secret.id])
        private = await memory.compact(actor, topic_id=topic.id, force=True)
        shared = await memory.compact(owner, force=True)
        assert private is not None and shared is not None
        assert private.source_event_ids == [secret.id]
        assert secret.id not in shared.source_event_ids and case.corrected not in shared.text
        assert memory.retrieve(other, case.corrected, include_tasks=False) == []
        with pytest.raises(DomainError) as caught:
            memory.retrieve(other, topic_id=topic.id)
        assert caught.value.code == "permission_denied"
    else:
        raise AssertionError("Unimplemented frozen case")

    # The formal task owner is the invariant in every independent group. A
    # recalled creation is intentionally removed, never reassigned to its peer.
    with store.transaction(owner) as tx:
        current_a, current_b = (tx.get_team_task(task.id) for task in tasks)
    assert current_b.owner_id == other.member_id
    assert current_b.title == case.task_b and current_b.status == TeamTaskState.OPEN
    assert memory.get_fact(other, facts[1].id).subject_id == other.member_id
    assert memory.get_fact(other, facts[1].id).value == case.task_b
    if scenario == "recall_then_replay":
        assert current_a.content_state in {"forgotten", "recalled"}
        assert current_a.owner_id is None and case.task_a not in current_a.title
    else:
        assert current_a.owner_id == actor.member_id
        assert current_a.status == TeamTaskState.OPEN
        assert current_a.title == (case.corrected if scenario == "own_correction" else case.task_a)
        assert memory.get_fact(actor, facts[0].id).subject_id == actor.member_id
