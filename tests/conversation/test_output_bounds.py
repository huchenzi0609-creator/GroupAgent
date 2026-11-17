"""Typed exact/minimum/maximum obligations preserve the actual user's bound."""

import json

import pytest
from jsonschema import Draft202012Validator
from pydantic import ValidationError

from groupagent.conversation.actions import OutputRequirement
from groupagent.conversation.provider import work_action_schema


@pytest.mark.parametrize(
    "requirement,accepted,rejected",
    [
        ({"exact_units": 2}, [2], [1, 3]),
        ({"min_units": 2}, [2, 3, 20], [1]),
        ({"max_units": 2}, [1, 2], [3, 20]),
        ({"min_units": 2, "max_units": 4}, [2, 3, 4], [1, 5]),
    ],
)
def test_actual_count_is_checked_against_the_requested_comparison(requirement, accepted, rejected):
    output = OutputRequirement(format="pdf", **requirement)
    assert all(output.accepts_unit_count(count) for count in accepted)
    assert not any(output.accepts_unit_count(count) for count in [*rejected, None, 0, True])
    assert OutputRequirement.model_validate_json(output.model_dump_json()) == output


def test_unspecified_count_never_adds_a_length_requirement():
    output = OutputRequirement(format="pdf")
    assert all(output.accepts_unit_count(count) for count in (None, 1, 2, 5))


@pytest.mark.parametrize(
    "requirement,code",
    [
        ({"exact_units": 2, "max_units": 2}, "exact_units_conflicts_with_range"),
        ({"exact_units": 2, "min_units": 1}, "exact_units_conflicts_with_range"),
        ({"min_units": 4, "max_units": 2}, "min_units_exceeds_max_units"),
    ],
)
def test_ambiguous_or_impossible_constraints_are_rejected_before_work_commit(requirement, code):
    with pytest.raises(ValidationError) as error:
        OutputRequirement(format="pdf", **requirement)
    assert error.value.errors(include_input=False)[0]["type"] == code


@pytest.mark.parametrize("field", ["exact_units", "min_units", "max_units"])
@pytest.mark.parametrize("value", [0, 201, True, 1.5, "2"])
def test_bounds_are_real_bounded_integers(field, value):
    with pytest.raises(ValidationError):
        OutputRequirement.model_validate({"format": "pdf", field: value})


def test_named_work_schemas_expose_bounds_without_an_exact_page_default():
    for kind in ("start_work", "update_work"):
        schema = work_action_schema(kind)
        Draft202012Validator.check_schema(schema)
        output = schema["properties"]["outputs"]["items"]
        assert {"exact_units", "min_units", "max_units"} <= set(output["properties"])
        assert output["properties"]["max_units"].get("default") is None
        assert "shorter valid document" in output["properties"]["max_units"]["description"]
        assert not {"exact_units", "min_units", "max_units"} & set(output["required"])
        value = {
            "goal": "Create the accepted PDF",
            "work_capabilities": ["documents"],
            "outputs": [{"format": "pdf", "max_units": 2}],
        }
        if kind == "update_work":
            value.update(target_work_id="synthetic-work", expected_revision=1)
        else:
            value["admission_ack"] = "deferred"
        Draft202012Validator(schema).validate(value)
        assert "exact_units" not in json.dumps(value)
