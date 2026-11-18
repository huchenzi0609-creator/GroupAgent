"""Normalize literal output assertions at the work boundary."""

import re
from collections.abc import Mapping, Sequence, Set
from typing import Any

_NUMBER_LITERAL = re.compile(r"[+\-−]?(?:\d+(?:\.\d+)?|\.\d+)(?:[eE][+\-−]?\d+)?[%％]?")


def final_output_requirements(
    outputs: list[dict[str, Any]],
    formats: Sequence[str],
    previous_outputs: Sequence[Any] = (),
) -> tuple[list[dict[str, Any]], bool]:
    """Keep authored finals and their typed requirements, including previously bound constraints."""
    selected = [row for row in outputs if row["format"] == "text" or row["format"] in formats]
    removed = len(selected) != len(outputs)
    included = {row["format"] for row in selected}
    previous = {
        row.get("format"): row
        for row in previous_outputs
        if isinstance(row, dict) and isinstance(row.get("format"), str)
    }
    selected.extend(
        previous.get(format_name, {"format": format_name})
        for format_name in formats
        if format_name not in included
    )
    return selected, removed


def _given_literal(text: str, literal: str) -> bool:
    if not literal.strip():
        return False
    if _NUMBER_LITERAL.fullmatch(literal):
        # Do not turn part of a larger number, a signed value, or an ID into
        # a source for an independently proposed numeric assertion.
        return bool(
            re.search(
                rf"(?<![A-Za-z_\d.+\-−]){re.escape(literal)}(?![A-Za-z_\d%％]|\.\d)",
                text,
            )
        )
    return literal in text


def literal_output_requirements(
    outputs: list[dict[str, Any]],
    source_text: str,
    *,
    carried_literals: Mapping[str, Set[str]] | None = None,
) -> list[dict[str, Any]]:
    """Gate live literals on human source, retaining the legacy table normalization."""
    if carried_literals is not None:
        return [
            {
                **output,
                "expected_text": list(
                    dict.fromkeys(
                        literal
                        for literal in output["expected_text"]
                        if isinstance(literal, str)
                        and (
                            literal in carried_literals.get(output["format"], ())
                            or _given_literal(source_text, literal)
                        )
                    )
                ),
            }
            if "expected_text" in output
            else output
            for output in outputs
        ]

    # Legacy decisions split simple label=value rows for an explicitly requested
    # table. Quoted or explicitly verbatim strings remain a single assertion.
    if not re.search(
        r"表格|数据表|(?:每|各|[一二三四五六七八九十0-9]+)(?:行|列)|\btable\b", source_text, re.I
    ):
        return outputs
    normalized = []
    for output in outputs:
        if output.get("format") not in {"docx", "pdf", "pptx"}:
            normalized.append(output)
            continue
        literals = []
        for value in output.get("expected_text", []):
            if not isinstance(value, str):
                literals.append(value)
                continue
            pair = re.fullmatch(r"([^=＝\n]{1,80}?)\s*[=＝]\s*([-+]?\d+(?:\.\d+)?)", value.strip())
            quoted = any(
                left + value + right in source_text
                for left, right in (('"', '"'), ("'", "'"), ("“", "”"), ("‘", "’"), ("`", "`"))
            )
            verbatim = bool(
                re.search(r"(?:字符串|逐字|字面|原样写).{0,12}" + re.escape(value), source_text)
            )
            if pair and not quoted and not verbatim:
                literals.extend((pair[1].strip(), pair[2]))
            else:
                literals.append(value)
        normalized.append({**output, "expected_text": list(dict.fromkeys(literals))})
    return normalized
