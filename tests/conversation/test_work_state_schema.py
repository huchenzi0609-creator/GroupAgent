from __future__ import annotations

from groupagent.control import WorkSelector
from groupagent.conversation.provider import decision_schema
from groupagent.domain import WorkState


def test_work_state_schema_defines_every_current_phase_without_changing_the_enum():
    definition = decision_schema()["$defs"]["WorkState"]
    assert definition["enum"] == [state.value for state in WorkState]
    assert definition["type"] == "string"
    for state in WorkState:
        assert state.value + "=" in definition["description"]
    assert "running=执行器正在执行工作" in definition["description"]
    assert "paused=执行已暂停，当前不在运行" in definition["description"]


def test_empty_state_set_remains_unfiltered_and_is_explained_as_such():
    field = decision_schema()["$defs"]["WorkSelector"]["properties"]["states"]
    assert "空数组表示所有状态、不作阶段过滤" in field["description"]
    assert field["items"] == {"$ref": "#/$defs/WorkState"}
    assert "minItems" not in field
    selector = WorkSelector(basis="query", quantifier="all", requester_scope="any", states=())
    assert selector.states == ()
