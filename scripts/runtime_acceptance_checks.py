"""Pure checks for the frozen synthetic runtime acceptance set.

No provider calls, environment credentials or product goal construction occur here.
Office/PDF inspection is delegated to a trusted script in the isolated worker.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shlex
import unicodedata
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

_STATUSES = {"PASS", "FAIL", "NOT_RUN", "NEEDS_REVIEW"}
_PROCESS_TOOLS = {"python_compute", "process_run", "shell_command"}


def _normal(value: Any) -> str:
    return re.sub(r"\s+", "", unicodedata.normalize("NFKC", str(value))).casefold()


def _date(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo is not None else None
    except ValueError:
        return None


def _url(value: str) -> str:
    try:
        parsed = urlsplit(value.rstrip(".,;，。；"))
        return urlunsplit(
            (parsed.scheme.lower(), parsed.netloc.lower(), parsed.path, parsed.query, "")
        )
    except ValueError:
        return value


def _valid_hash(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[a-f0-9]{64}", value) is not None


def _canonical_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


def _independent_ocr_texts(case_id: str, evidence: dict[str, Any]) -> tuple[list[str], bool]:
    """Extra OCR needs actual source/response/settled-budget bindings, never a review."""
    observations = evidence.get("independent_ocr_observations", [])
    references = evidence.get("independent_ocr_observation_sources", [])
    texts = []
    if not isinstance(observations, list) or not isinstance(references, list):
        return [], False
    if len(observations) != len(references):
        return [], False
    scope = evidence.get("scope", {})
    try:
        for item, reference in zip(observations, references, strict=True):
            source, response, receipt = (
                item["source"],
                item["provider_response"],
                item["budget_receipt"],
            )
            artifacts = [
                artifact
                for artifact in evidence.get("artifacts", [])
                if all(artifact.get(key) == source.get(key) for key in ("file_id", "version_id"))
            ]
            if len(artifacts) != 1:
                return [], False
            artifact = artifacts[0]
            image = artifact.get("decoded", {}).get("image", {})
            page = response["pages"][0]
            expected_key = (
                f"acceptance-ocr:{item['case_id']}:{source['file_id']}:"
                f"{source['version_id']}:{source['sha256']}"
            )
            if not (
                item.get("kind") == "independent_machine_ocr"
                and item.get("case_id") == case_id
                and item.get("observer") == "scripts/runtime_png_ocr.py"
                and item.get("status") == "completed"
                and item.get("method") == "deepseek_ocr"
                and item.get("provider") == "deepseek"
                and item.get("model") == "tools/ocr"
                and item.get("transport") == "live_http"
                and item.get("request_contains_gold_or_prompt") is False
                and item.get("input_filename") == "image.png"
                and item.get("business_writes_applied") is False
                and item.get("budget_receipt_read_from_database") is True
                and all(
                    scope.get(key) for key in ("group_id", "work_id", "event_id", "requester_id")
                )
                and item.get("scope") == scope
                and all(artifact.get(key) == scope[key] for key in ("group_id", "work_id"))
                and _date(evidence.get("delivery", {}).get("work_completed_at")) is not None
                and artifact.get("format") == "png"
                and _valid_hash(source.get("sha256"))
                and source["sha256"] == artifact.get("sha256")
                and source["sha256"] == artifact.get("decoded", {}).get("source_sha256")
                and type(source.get("size_bytes")) is int
                and source["size_bytes"] > 0
                and source["size_bytes"] == artifact.get("size_bytes")
                and artifact.get("validation", {}).get("passed") is True
                and source["sha256"] == artifact["validation"].get("sha256")
                and source.get("validation_id") == artifact["validation"].get("validation_id")
                and bool(source.get("validation_id"))
                and image.get("decoded") is True
                and _valid_hash(image.get("pixel_sha256"))
                and item.get("input_pixel_sha256")
                == source.get("pixel_sha256")
                == image["pixel_sha256"]
                and _valid_hash(item.get("input_sha256"))
                and item.get("original_local_ocr_text") == image.get("ocr_text")
                and all(
                    _valid_hash(item.get(key, {}).get("sha256")) and item[key].get("path")
                    for key in (
                        "source_evidence",
                        "source_progress",
                        "source_manifest",
                        "immutable_version_record",
                    )
                )
                and _valid_hash(reference.get("sha256"))
                and reference.get("path")
                and reference.get("payload_sha256") == _canonical_hash(item)
                and item.get("provider_response_sha256") == _canonical_hash(response)
                and isinstance(response.get("request_id"), str)
                and response["request_id"]
                and len(response["pages"]) == 1
                and page.get("page_number") == 1
                and page.get("method") == "deepseek_ocr"
                and page.get("complete") is True
                and isinstance(page.get("text"), str)
                and item.get("text") == page["text"]
                and response.get("usage") == item.get("usage") == {"pages": 1.0}
                and item.get("usage_method") == "complete_page_count_from_provider_response"
                and receipt.get("id") == item.get("reservation_id")
                and receipt["id"]
                and receipt.get("group_id") == scope["group_id"]
                and receipt.get("actor_id") == scope["requester_id"]
                and receipt.get("source_event_id") == scope["event_id"]
                and receipt.get("work_id") is None
                and receipt.get("key") == expected_key
                and receipt.get("operation") == "media.ocr"
                and receipt.get("price_id") == "deepseek-20261114-media.ocr"
                and receipt.get("currency") == "CNY"
                and receipt.get("state") == "settled"
                and receipt.get("overrun") is False
                and receipt.get("evidence_ref")
                == item.get("budget_evidence_ref")
                == "deepseek:" + response["request_id"]
                and Decimal(str(receipt["estimated_units"]["pages"])) == 1
                and Decimal(str(receipt["actual_units"]["pages"])) == 1
                and Decimal(str(receipt["reserved_amount"])) == Decimal("0.0015")
                and Decimal(str(receipt["actual_amount"])) == Decimal("0.0015")
            ):
                return [], False
            texts.append(page["text"])
    except (AttributeError, TypeError, KeyError, ValueError, IndexError, InvalidOperation):
        return [], False
    return texts, True


def _markdown_edges(markdown: str) -> set[tuple[str, str]]:
    """Read literal arrows and scoped Mermaid aliases, without executing code.

    Ordinary prose such as ``Start (top)`` must not rename every ``Start``
    elsewhere in a Markdown document. Each declared Mermaid graph has its own
    alias map; plain-text edges retain their actual endpoint names.
    """
    node = re.compile(r"([A-Za-z][A-Za-z0-9_-]*)\s*[\[({]\s*[\"']?([^\])}\n\"']+)[\"']?\s*[\])}]")

    def read(text: str, *, graph: bool = False) -> set[tuple[str, str]]:
        labels = {}
        if graph:
            text = re.sub(r"(?m)^[ \t]*%%[^\n]*$", "", text)
            labels = {match[1]: match[2].strip() for match in node.finditer(text)}
            text = node.sub(lambda match: match[1], text)
        return {
            (_normal(labels.get(match[1], match[1])), _normal(labels.get(match[2], match[2])))
            for match in re.finditer(
                r"(?<![A-Za-z0-9_-])(?=([A-Za-z][A-Za-z0-9_-]*)\s*(?:-->|→|->|=>|➜)(?:\|[^|\n]{0,100}\|)?\s*([A-Za-z][A-Za-z0-9_-]*))",
                text,
            )
        }

    def plain_blocks(text: str) -> set[tuple[str, str]]:
        result = set()
        for block in re.split(r"\n[ \t]*\n", text):
            declared = bool(re.match(r"\s*(?:graph|flowchart)\s+(?:TD|TB|BT|LR|RL)\b", block))
            result.update(read(block, graph=declared))
        return result

    result: set[tuple[str, str]] = set()
    outside: list[str] = []
    lines = markdown.splitlines()
    index = 0
    while index < len(lines):
        fence = re.fullmatch(
            r"[ \t]{0,3}(`{3,}|~{3,})[ \t]*([A-Za-z0-9_-]*)(?:[ \t]+[^\n]*)?", lines[index]
        )
        if fence is None:
            outside.append(lines[index])
            index += 1
            continue
        result.update(plain_blocks("\n".join(outside)))
        outside.clear()
        marker, language = fence.groups()
        closing = re.compile(
            r"[ \t]{0,3}" + re.escape(marker[0]) + "{" + str(len(marker)) + r",}[ \t]*"
        )
        body = []
        index += 1
        while index < len(lines) and closing.fullmatch(lines[index]) is None:
            body.append(lines[index])
            index += 1
        result.update(read("\n".join(body), graph=language.lower() == "mermaid"))
        index += 1
    result.update(plain_blocks("\n".join(outside)))
    return result


def evaluate_case(case: dict[str, Any], evidence: dict[str, Any]) -> dict[str, Any]:
    """Malformed/missing evidence never crashes a matrix or becomes a pass."""
    try:
        return _evaluate_case(case, evidence)
    except (AttributeError, TypeError, KeyError, ValueError, IndexError) as error:
        return {
            "case_id": case["id"],
            "status": "NOT_RUN",
            "checks": {
                key: {"status": "NOT_RUN", "detail": "Evidence structure requires review"}
                for key in case["checks"]
            },
            "checker_error": {"status": "NEEDS_REVIEW", "error_type": type(error).__name__},
            "business_writes_applied_by_checker": False,
        }


def _evaluate_case(case: dict[str, Any], evidence: dict[str, Any]) -> dict[str, Any]:
    """Return one explicit result for every frozen check; absent evidence is NOT_RUN.

    Evidence keys: scope, answer_text, artifacts, tool_receipts, input_media,
    execution_backend, delivery, scheduling, before_after, comparison, reviews.
    Each tool_receipts entry wraps {group_id, work_id, receipt, arguments?}.
    Artifacts carry an actual immutable identity, validation and decoded data.
    Reviews are explicit {passed:bool,evidence_ref:str} attestations.
    """
    expected = case["checks"]
    scope = evidence.get("scope", {})
    artifacts = evidence.get("artifacts")
    artifacts = artifacts if isinstance(artifacts, list) else None
    outputs = artifacts or []
    wrappers = evidence.get("tool_receipts")
    wrappers = wrappers if isinstance(wrappers, list) else None
    scoped = [
        item
        for item in wrappers or []
        if isinstance(item, dict)
        and item.get("group_id") == scope.get("group_id")
        and item.get("work_id") == scope.get("work_id")
    ]
    receipts = [item for item in scoped if isinstance(item.get("receipt"), dict)]
    success = [item for item in receipts if item["receipt"].get("success") is True]
    names = {item["receipt"].get("tool_name") for item in success}
    processes = [
        item["receipt"].get("output", {})
        for item in success
        if item["receipt"].get("tool_name") in _PROCESS_TOOLS
    ]
    processes = [item for item in processes if isinstance(item, dict)]
    decoded = [item.get("decoded", {}) for item in outputs if isinstance(item, dict)]
    texts = [evidence["answer_text"]] if isinstance(evidence.get("answer_text"), str) else []
    texts += [item["text"] for item in decoded if isinstance(item.get("text"), str)]
    if case["id"].startswith("RA-ASR"):
        texts += [
            item["transcript"]
            for item in evidence.get("input_media", [])
            if isinstance(item, dict) and isinstance(item.get("transcript"), str)
        ]
    text = "\n".join(texts)
    normalized = _normal(text)
    input_media = evidence.get("input_media")
    input_media = input_media if isinstance(input_media, list) else None
    images = [item["image"] for item in decoded if isinstance(item.get("image"), dict)]
    audios = [item["audio"] for item in decoded if isinstance(item.get("audio"), dict)]
    tables = [
        table for item in decoded for table in item.get("tables", []) if isinstance(table, list)
    ]
    result: dict[str, dict[str, Any]] = {}
    independent_ocr, independent_ocr_valid = _independent_ocr_texts(case["id"], evidence)

    def record(key: str, passed: bool | None, detail: str = "", *, review: bool = False) -> None:
        result[key] = {
            "status": "NEEDS_REVIEW"
            if review
            else "NOT_RUN"
            if passed is None
            else "PASS"
            if passed
            else "FAIL",
            "detail": detail,
        }

    def need_review(key: str, expected_value: Any) -> None:
        review = evidence.get("reviews", {}).get(key)
        if (
            isinstance(review, dict)
            and type(review.get("passed")) is bool
            and isinstance(review.get("evidence_ref"), str)
            and review["evidence_ref"]
        ):
            record(key, review["passed"], "Explicit review: " + review["evidence_ref"])
        else:
            record(
                key,
                None,
                "Independent review required: " + json.dumps(expected_value, ensure_ascii=False),
                review=True,
            )

    if "independent_ocr_observations" in evidence:
        record(
            "_independent_ocr_observation",
            independent_ocr_valid,
            "Additional machine OCR requires matching pixels, scope, provider response and settled ledger receipt",
        )

    # Integrity checks are additional evidence gates, never substitutions for gold.
    if outputs:
        scope_known = bool(scope.get("group_id") and scope.get("work_id")) and all(
            (item.get("group_id") or item.get("provenance", {}).get("group_id"))
            and (item.get("work_id") or item.get("provenance", {}).get("work_id"))
            for item in outputs
        )
        record(
            "_artifact_scope",
            all(
                (item.get("group_id") or item.get("provenance", {}).get("group_id"))
                == scope["group_id"]
                and (item.get("work_id") or item.get("provenance", {}).get("work_id"))
                == scope["work_id"]
                for item in outputs
            )
            if scope_known
            else None,
            "Only selected results in the case group/work count",
        )
        known = all(
            _valid_hash(item.get("sha256"))
            and isinstance(item.get("decoded"), dict)
            and _valid_hash(item["decoded"].get("source_sha256"))
            for item in outputs
        )
        record(
            "_artifact_bytes_bound",
            all(item["sha256"] == item["decoded"]["source_sha256"] for item in outputs)
            if known
            else None,
            "Decoded bytes must match immutable artifact hashes",
        )
        validation_known = all(isinstance(item.get("validation"), dict) for item in outputs)
        record(
            "_artifact_validation",
            all(
                item["validation"].get("passed") is True
                and item["validation"].get("sha256") == item.get("sha256")
                for item in outputs
            )
            if validation_known
            else None,
            "Delivery needs matching actual validation records",
        )
    if wrappers is not None:
        record(
            "_receipt_scope",
            len(scoped) == len(wrappers) if scope.get("group_id") else None,
            "Only this case's group/work receipts may count",
        )

    source_map: dict[str, dict[str, Any]] = {}
    for item in success:
        receipt = item["receipt"]
        body = receipt.get("output") or {}
        if not isinstance(body, dict):
            continue
        entries = (
            body.get("results", [])
            if receipt.get("tool_name") == "web_search"
            else [body.get("source")]
            if receipt.get("tool_name") == "web_fetch"
            else []
        )
        for source in entries:
            if isinstance(source, dict) and isinstance(source.get("url"), str):
                source_map[_url(source["url"])] = source
    # Bare URLs stop at prose delimiters, including full-width/CJK punctuation.
    # Do not stop at Chinese letters: legitimate Unicode paths/query text remain
    # intact. Structured hyperlink targets below are not subject to this lexer;
    # percent-encoded punctuation is also preserved.
    citations = {
        _url(value)
        for value in re.findall(
            r"https?://[^\s<>\]\)\"'，。；：！？、（）【】《》〈〉“”‘’「」『』〔〕［］｛｝]+",
            text,
            re.I,
        )
    }
    for item in decoded:
        citations.update(_url(value) for value in item.get("links", []) if isinstance(value, str))
    matched = citations & source_map.keys()

    def page_values(unit_kind: str = "rendered_pages") -> list[int] | None:
        values = []
        selected_formats = set(expected.get("expected_formats", [expected.get("expected_format")]))
        for item in outputs:
            if item.get("format") not in selected_formats:
                continue
            parsed = item.get("decoded", {})
            value = (
                parsed.get("unit_count")
                if unit_kind in {"slides", "pages"}
                else parsed.get("rendered_pages")
            )
            if value is None and unit_kind == "rendered_pages":
                validation = item.get("validation", {})
                if (
                    validation.get("rendered") is True
                    and isinstance(validation.get("preview_sha256"), (list, tuple))
                    and len(validation["preview_sha256"]) == validation.get("page_count")
                ):
                    value = validation.get("page_count")
            if type(value) is not int:
                return None
            values.append(value)
        return values or None

    schedule = evidence.get("scheduling", {})
    rule = schedule.get("rule")
    comparison_value = evidence.get("comparison")
    comparison = comparison_value if isinstance(comparison_value, dict) else {}
    before_after = evidence.get("before_after", {})
    delivery = evidence.get("delivery", {})
    for key, value in expected.items():
        if key in {"unit_kind", "time_anchor", "due_tolerance_seconds"}:
            continue  # Evaluated with their owning assertion below.
        if key in {"expected_format", "expected_formats"}:
            formats = {item.get("format") for item in outputs}
            wanted = {value} if key == "expected_format" else set(value)
            record(
                key, wanted <= formats if artifacts is not None else None, "Actual artifact formats"
            )
        elif key in {"required_text", "forbidden_text", "any_of_text_groups", "required_regex"}:
            if not texts:
                record(key, None, "Actual answer/artifact text is absent")
            elif key == "required_text":
                missing = [part for part in value if _normal(part) not in normalized]
                record(key, not missing, "Missing: " + json.dumps(missing, ensure_ascii=False))
            elif key == "forbidden_text":
                found = [part for part in value if _normal(part) in normalized]
                record(key, not found, "Unexpected text: " + json.dumps(found, ensure_ascii=False))
            elif key == "any_of_text_groups":
                record(
                    key,
                    all(any(_normal(part) in normalized for part in group) for group in value),
                    "Each allowed-word group must be represented",
                )
            else:
                record(
                    key,
                    all(re.search(pattern, text, re.I) is not None for pattern in value),
                    "Required output patterns",
                )
        elif key == "numeric_answer":
            numbers = re.findall(r"(?<![\w.])-?\d+(?:\.\d+)?(?![\w.])", text, flags=re.ASCII)
            record(
                key,
                any(float(number) == float(value) for number in numbers) if texts else None,
                "Numeric answer in actual output; multi-claim meaning is not inferred",
            )
        elif key == "exact_units":
            counts = page_values(expected.get("unit_kind", "rendered_pages"))
            record(
                key,
                all(count == value for count in counts) if counts else None,
                "Actual decoded slides/pages or matching rendered-page evidence",
            )
        elif key == "rendered_page_range":
            counts = page_values()
            record(
                key,
                all(value[0] <= count <= value[1] for count in counts) if counts else None,
                "Actual rendered page count",
            )
        elif key == "per_unit_text":
            units = [
                unit for item in decoded for unit in item.get("units", []) if isinstance(unit, dict)
            ]
            if not units:
                record(key, None, "Per-unit decoded text is absent")
            else:
                record(
                    key,
                    all(
                        any(
                            unit.get("index") == int(index)
                            and all(
                                _normal(part) in _normal(unit.get("text", "")) for part in parts
                            )
                            for unit in units
                        )
                        for index, parts in value.items()
                    ),
                    "One-based slide/page text checks",
                )
        elif key in {
            "table_min_rows_including_header",
            "native_table_count_min",
            "table_expected_rows",
        }:
            table_evidence = bool(decoded) and all("tables" in item for item in decoded)
            if not table_evidence:
                record(key, None, "Native table inspection was not performed")
            elif key == "table_min_rows_including_header":
                record(
                    key,
                    any(len(table) >= value for table in tables),
                    "Rows in a native table, including its header",
                )
            elif key == "native_table_count_min":
                record(key, len(tables) >= value, "Native tables, not screenshots or prose")
            else:
                rows = [
                    [_normal(cell) for cell in row]
                    for table in tables
                    for row in table
                    if isinstance(row, list)
                ]
                record(
                    key,
                    all([_normal(cell) for cell in row] in rows for row in value),
                    "Expected native table rows",
                )
        elif key.startswith("markdown_") or key == "json_codeblock_equals":
            markdowns = [
                item.get("decoded", {}).get("text")
                for item in outputs
                if item.get("format") == "md"
            ]
            markdowns = [item for item in markdowns if isinstance(item, str)]
            if not markdowns:
                record(key, None, "Actual Markdown bytes/text are absent")
                continue
            markdown = "\n".join(markdowns)
            if key == "markdown_h2":
                headings = {
                    _normal(line) for line in re.findall(r"^##\s+(.+?)\s*#*\s*$", markdown, re.M)
                }
                record(key, {_normal(item) for item in value} <= headings, "Actual H2 headings")
            elif key == "markdown_table_min_data_rows":
                lines = markdown.splitlines()
                counts = []
                for index, line in enumerate(lines):
                    if re.fullmatch(r"\s*\|?\s*:?-{3,}:?\s*(?:\|\s*:?-{3,}:?\s*)+\|?\s*", line):
                        count = 0
                        for row in lines[index + 1 :]:
                            if "|" not in row or not row.strip():
                                break
                            count += 1
                        counts.append(count)
                record(key, bool(counts) and max(counts) >= value, "Markdown table data-row count")
            elif key == "json_codeblock_equals":
                found = []
                for raw in re.findall(r"```json\s*\n(.*?)\n```", markdown, re.S | re.I):
                    try:
                        found.append(json.loads(raw))
                    except ValueError:
                        pass
                record(key, any(item == value for item in found), "Parsed fenced JSON equality")
            elif key == "markdown_required_edges":
                record(
                    key,
                    {(_normal(left), _normal(right)) for left, right in value}
                    <= _markdown_edges(markdown),
                    "Explicit directed edges in Markdown",
                )
            else:
                record(key, None, "Unsupported Markdown assertion")
        elif key in {
            "successful_tools_all",
            "successful_tools_any",
            "forbidden_tools",
            "no_research_call_required",
        }:
            if wrappers is None:
                record(key, None, "Actual scoped tool ledger is absent")
            elif key == "successful_tools_all":
                record(key, set(value) <= names, "Successful scoped tool receipts")
            elif key == "successful_tools_any":
                record(key, bool(set(value) & names), "Successful scoped tool receipts")
            elif key == "forbidden_tools":
                record(
                    key,
                    not any(item["receipt"].get("tool_name") in value for item in receipts),
                    "Includes attempted failed calls",
                )
            else:
                record(key, True, "Gold says research is not required; absence is not a failure")
        elif key == "process_exit_code":
            record(
                key,
                all(item.get("exit_code") == value for item in processes) if processes else None,
                "Actual successful process receipts",
            )
        elif key == "stdout_json_equals":
            parsed = []
            for item in processes:
                try:
                    parsed.append(json.loads(item.get("stdout", "")))
                except ValueError:
                    pass
            record(
                key,
                any(item == value for item in parsed) if processes else None,
                "JSON parsed from actual stdout",
            )
        elif key in {"stdout_os_release_keys", "answer_matches_stdout_values"}:
            values = {}
            for process in processes:
                for line in process.get("stdout", "").splitlines():
                    if re.fullmatch(r"[A-Z_]+=.*", line):
                        name, raw = line.split("=", 1)
                        try:
                            shell_values = shlex.split(raw)
                        except ValueError:
                            continue
                        if len(shell_values) == 1:
                            values[name] = shell_values[0]
            passed = all(name in values for name in value)
            if key == "answer_matches_stdout_values":
                passed = passed and all(_normal(values[name]) in normalized for name in value)
            record(key, passed if processes else None, "OS release values must originate in stdout")
        elif key == "execution_backend":
            record(
                key,
                evidence.get("execution_backend") == value
                if "execution_backend" in evidence
                else None,
                "Trusted runner backend evidence",
            )
        elif key == "image_min_dimensions":
            record(
                key,
                all(
                    item.get("decoded") is True
                    and item.get("width", 0) >= value[0]
                    and item.get("height", 0) >= value[1]
                    for item in images
                )
                if images
                else None,
                "Decoded output dimensions",
            )
        elif key == "image_nonblank":
            record(
                key,
                all(item.get("nonblank") is True for item in images) if images else None,
                "Actual raster pixel contrast",
            )
        elif key == "image_ocr_required_text":
            local_texts = [item.get("ocr_text", "") for item in images]
            observed_texts = local_texts + independent_ocr
            record(
                key,
                all(
                    any(_normal(part) in _normal(observed) for observed in observed_texts)
                    for part in value
                )
                if images and all("ocr_text" in item for item in images)
                else None,
                "OCR of actual output pixels; original local extraction is retained",
            )
            if independent_ocr:
                result[key]["original_local_observation"] = {
                    "status": "PASS"
                    if all(
                        any(_normal(part) in _normal(observed) for observed in local_texts)
                        for part in value
                    )
                    else "FAIL",
                    "detail": "Original local OCR remains unchanged; additional OCR is independently receipted",
                }
        elif key == "chart_data_equals":
            charts = [
                item["arguments"].get("chart")
                for item in success
                if item["receipt"].get("tool_name") == "chart_render"
                and isinstance(item.get("arguments"), dict)
                and isinstance(item["receipt"].get("output"), dict)
                and any(
                    artifact.get("version_id") == item["receipt"]["output"].get("version_id")
                    and artifact.get("sha256") == item["receipt"]["output"].get("sha256")
                    for artifact in outputs
                )
            ]
            charts += [
                item["provenance"].get("chart_spec")
                for item in outputs
                if isinstance(item.get("provenance"), dict)
            ]
            usable = [
                item
                for item in charts
                if isinstance(item, dict)
                and isinstance(item.get("kind"), str)
                and isinstance(item.get("categories"), list)
                and all(isinstance(category, str) for category in item["categories"])
                and isinstance(item.get("series"), list)
                and item["series"]
                and all(
                    isinstance(series, dict)
                    and isinstance(series.get("values"), list)
                    and all(
                        type(point) in {int, float} and math.isfinite(point)
                        for point in series["values"]
                    )
                    for series in item["series"]
                )
            ]
            matching_chart = any(
                item.get("kind") == value["kind"]
                and list(item.get("categories", [])) == value["categories"]
                and len(item.get("series", [])) == 1
                and list(item["series"][0].get("values", [])) == value["values"]
                for item in usable
            )
            # Free-form Python source is not parsed as a trustworthy chart data manifest.
            if not usable and images and "python_compute" in names:
                need_review(key, value)
            else:
                record(
                    key,
                    matching_chart if usable else None,
                    "Successful chart tool arguments or bound stored chart specification required",
                )
        elif key in {
            "actual_pixel_input_required",
            "actual_audio_bytes_required",
            "asr_provider_receipt_required",
            "source_attachment_binding_required",
        }:
            if not input_media:
                record(key, None, "Actual input-byte/source evidence is absent")
            elif key == "actual_pixel_input_required":
                record(
                    key,
                    any(
                        _valid_hash(item.get("pixel_input_sha256"))
                        and _valid_hash(item.get("source_sha256"))
                        for item in input_media
                    ),
                    "Verified actual pixel input hashes",
                )
            elif key == "actual_audio_bytes_required":
                record(
                    key,
                    any(
                        _valid_hash(item.get("audio_input_sha256"))
                        and _valid_hash(item.get("source_sha256"))
                        for item in input_media
                    ),
                    "Verified actual audio input hashes",
                )
            elif key == "asr_provider_receipt_required":
                record(
                    key,
                    any(
                        isinstance(item.get("provider_receipt"), str)
                        and bool(item["provider_receipt"])
                        for item in input_media
                    ),
                    "Actual ASR provider/budget receipt",
                )
            else:
                record(
                    key,
                    all(
                        item.get("binding_verified") is True
                        and item.get("event_id")
                        and item.get("attachment_id")
                        and _valid_hash(item.get("source_sha256"))
                        for item in input_media
                    ),
                    "Canonical attachment/source association verified by runner",
                )
        elif key == "audio_decode_required":
            record(
                key,
                all(item.get("decoded") is True for item in audios) if audios else None,
                "Actual audio decode",
            )
        elif key == "audio_duration_seconds_range":
            record(
                key,
                all(
                    isinstance(item.get("duration_seconds"), (int, float))
                    and math.isfinite(item["duration_seconds"])
                    and value[0] <= item["duration_seconds"] <= value[1]
                    for item in audios
                )
                if audios
                else None,
                "Actual audio duration",
            )
        elif key == "audio_non_silent":
            record(
                key,
                all(item.get("non_silent") is True for item in audios) if audios else None,
                "Decoded PCM samples",
            )
        elif key in {"speech_content_check_required", "transcript_any_of_text_groups"}:
            transcripts = []
            mismatched_audio = False
            for artifact in outputs:
                item = artifact.get("decoded", {}).get("audio")
                if not isinstance(item, dict):
                    continue
                asr_hash = item.get("asr_source_sha256", item.get("asr_input_sha256"))
                if asr_hash is not None and asr_hash != artifact.get("sha256"):
                    mismatched_audio = True
                if (
                    isinstance(item.get("asr_transcript"), str)
                    and item.get("asr_receipt")
                    and _valid_hash(asr_hash)
                    and asr_hash == artifact.get("sha256")
                ):
                    transcripts.append(item["asr_transcript"])
            if mismatched_audio:
                record(key, False, "ASR evidence describes different output audio bytes")
            elif not transcripts or len(transcripts) != len(audios):
                record(key, None, "Output audio needs independent real ASR transcript and receipt")
            elif key == "speech_content_check_required":
                record(
                    key,
                    all(bool(item.strip()) for item in transcripts),
                    "Independent actual output-speech transcript",
                )
            else:
                record(
                    key,
                    all(
                        any(
                            _normal(part) in _normal(transcript)
                            for part in group
                            for transcript in transcripts
                        )
                        for group in value
                    ),
                    "Each expected semantic group in actual output transcript",
                )
        elif key == "file_fallback_allowed":
            parts = delivery.get("parts")
            record(
                key,
                bool(value) or all(item.get("wire_kind") != "file" for item in parts)
                if isinstance(parts, list)
                else None,
                "Audio-file fallback is allowed only by the frozen requirement",
            )
        elif key in {"semantic_claims", "visual_review_required"}:
            need_review(key, value)
        elif key in {"new_human_tasks", "new_schedules"}:
            prefix = "human_tasks" if key == "new_human_tasks" else "schedules"
            before, after = (
                before_after.get(prefix + "_before"),
                before_after.get(prefix + "_after"),
            )
            record(
                key,
                after - before == value if type(before) is int and type(after) is int else None,
                "Actual database counts before/after",
            )
        elif key == "new_time_rules":
            before, after = schedule.get("rules_before"), schedule.get("rules_after")
            record(
                key,
                len({item["id"] for item in after} - {item["id"] for item in before}) == value
                if isinstance(before, list) and isinstance(after, list)
                else None,
                "Actual newly persisted rule IDs",
            )
        elif key == "same_rule_as":
            prior = schedule.get("reference_rule")
            record(
                key,
                rule.get("id") == prior.get("id") and schedule.get("reference_case_id") == value
                if isinstance(rule, dict)
                and isinstance(prior, dict)
                and schedule.get("reference_case_id")
                else None,
                "Prior-case rule identity",
            )
        elif key == "revision_increased":
            prior = schedule.get("reference_rule")
            record(
                key,
                rule["revision"] > prior["revision"]
                if isinstance(rule, dict)
                and isinstance(prior, dict)
                and type(rule.get("revision")) is int
                and type(prior.get("revision")) is int
                else None,
                "Actual rule revision",
            )
        elif key == "timing_kind":
            record(
                key,
                rule.get("timing", {}).get("kind") == value if isinstance(rule, dict) else None,
                "Persisted timing discriminator",
            )
        elif key == "relative_due_seconds":
            anchor = _date(scope.get("occurred_at"))
            due = _date(rule.get("timing", {}).get("at")) if isinstance(rule, dict) else None
            record(
                key,
                abs((due - anchor).total_seconds() - value)
                <= expected.get("due_tolerance_seconds", 0)
                if due and anchor
                else None,
                "Compared with canonical source-event time",
            )
        elif key == "goal_required_text":
            record(
                key,
                all(_normal(part) in _normal(rule.get("goal", "")) for part in value)
                if isinstance(rule, dict)
                else None,
                "Persisted rule goal",
            )
        elif key == "creator_is_requester":
            record(
                key,
                rule.get("creator_id") == scope.get("requester_id")
                if isinstance(rule, dict) and scope.get("requester_id")
                else None,
                "Persisted creator identity",
            )
        elif key == "rule_state":
            record(
                key,
                rule.get("state") == value if isinstance(rule, dict) else None,
                "Persisted rule state",
            )
        elif key in {
            "processing_same_event_twice_keeps_one_rule",
            "duplicate_tick_creates_no_extra_work_or_delivery",
        }:
            before, after = (
                schedule.get("duplicate_counts_before"),
                schedule.get("duplicate_counts_after"),
            )
            fields = (
                ("rules",)
                if key == "processing_same_event_twice_keeps_one_rule"
                else ("works", "occurrences", "deliveries")
            )
            record(
                key,
                all(before[field] == after[field] for field in fields)
                if isinstance(before, dict)
                and isinstance(after, dict)
                and all(field in before and field in after for field in fields)
                else None,
                "Actual replay/tick counts",
            )
        elif key in {"new_enqueued_occurrences", "new_works_from_referenced_rule"}:
            items = schedule.get("occurrences" if key == "new_enqueued_occurrences" else "works")
            reference = (schedule.get("reference_rule") or rule or {}).get("id")
            if items is None or not reference:
                record(key, None, "Actual rule-derived records absent")
            elif key == "new_enqueued_occurrences":
                record(
                    key,
                    len(
                        [
                            item
                            for item in items
                            if item.get("schedule_id") == reference and item.get("work_id")
                        ]
                    )
                    == value,
                    "Actual enqueued occurrence records",
                )
            else:
                record(
                    key,
                    len(
                        [
                            item
                            for item in items
                            if item.get("scope", {}).get("schedule_id") == reference
                            or item.get("scope", {}).get("origin_id") == reference
                            or item.get("scope", {}).get("origin_schedule_id") == reference
                        ]
                    )
                    == value,
                    "Actual work-rule source association",
                )
        elif key == "derived_depth":
            works = schedule.get("works")
            record(
                key,
                bool(works)
                and all(item.get("scope", {}).get("derived_depth") == value for item in works)
                if isinstance(works, list)
                else None,
                "Persisted derived-work depth",
            )
        elif key == "all_delivery_parts_accepted":
            parts = delivery.get("parts")
            record(
                key,
                bool(parts) and all(item.get("state") == "accepted" for item in parts)
                if isinstance(parts, list)
                else None,
                "Actual outbox/channel receipts, not reading confirmation",
            )
        elif key == "work_completed_after_accepted_delivery":
            completed = _date(delivery.get("work_completed_at"))
            parts = delivery.get("parts")
            accepted = (
                [_date(item.get("accepted_at") or item.get("updated_at")) for item in parts]
                if isinstance(parts, list)
                else []
            )
            record(
                key,
                all(item.get("state") == "accepted" for item in parts)
                and all(stamp <= completed for stamp in accepted if stamp)
                if completed and parts and all(accepted)
                else None,
                "Durable completion time must follow accepted parts",
            )
        elif key == "new_version_required":
            source = comparison.get("source_artifact")
            new = comparison.get("new_artifact")
            record(
                key,
                source.get("version_id") != new.get("version_id")
                if isinstance(source, dict)
                and isinstance(new, dict)
                and source.get("version_id")
                and new.get("version_id")
                else None,
                "Immutable source and result identities",
            )
        elif key == "source_bytes_unchanged":
            source_value = comparison.get("source_artifact")
            source = source_value if isinstance(source_value, dict) else {}
            after = comparison.get("source_sha256_after")
            record(
                key,
                source["sha256"] == after
                if _valid_hash(source.get("sha256")) and _valid_hash(after)
                else None,
                "Source bytes re-read after editing",
            )
        elif key == "source_provenance_chain_required":
            source_value, new_value = (
                comparison.get("source_artifact"),
                comparison.get("new_artifact"),
            )
            source = source_value if isinstance(source_value, dict) else {}
            new = new_value if isinstance(new_value, dict) else {}
            chain = comparison.get("provenance_version_ids")
            record(
                key,
                source.get("version_id") in chain and new.get("version_id") in chain
                if isinstance(chain, list) and source.get("version_id") and new.get("version_id")
                else None,
                "Runner-resolved actual version/provenance chain",
            )
        elif key == "unchanged_source_units":
            source_value, new_value = (
                comparison.get("source_artifact"),
                comparison.get("new_artifact"),
            )
            source_decoded = source_value.get("decoded") if isinstance(source_value, dict) else None
            new_decoded = new_value.get("decoded") if isinstance(new_value, dict) else None
            source = source_decoded if isinstance(source_decoded, dict) else {}
            new = new_decoded if isinstance(new_decoded, dict) else {}
            before = {
                item.get("index"): item.get("content_sha256") for item in source.get("units", [])
            }
            after = {item.get("index"): item.get("content_sha256") for item in new.get("units", [])}
            record(
                key,
                all(before[index] == after[index] for index in value)
                if all(
                    _valid_hash(before.get(index)) and _valid_hash(after.get(index))
                    for index in value
                )
                else None,
                "Actual unchanged unit XML/content hashes",
            )
        elif key == "citations_min":
            record(
                key,
                len(citations) >= value if texts else None,
                "Citations present in actual output",
            )
        elif key == "citation_hosts_all":
            record(
                key,
                set(value) <= {urlsplit(url).hostname for url in citations} if texts else None,
                "Required citation hosts",
            )
        elif key == "citation_path_suffix":
            record(
                key,
                any(urlsplit(url).path.endswith(value) for url in citations) if texts else None,
                "Required cited page path",
            )
        elif key == "citations_must_match_tool_provenance":
            record(
                key,
                bool(citations) and citations <= source_map.keys()
                if wrappers is not None and texts
                else None,
                "Every citation must match an actual successful research source",
            )
        elif key == "citation_access_time_required":
            dates = [_date(source_map[url].get("retrieved_at")) for url in matched]

            def represented(stamp: datetime) -> bool:
                return any(
                    token in text
                    for token in (
                        stamp.strftime("%Y-%m-%d"),
                        stamp.strftime("%Y/%m/%d"),
                        f"{stamp.year}年{stamp.month}月{stamp.day}日",
                    )
                )

            record(
                key,
                bool(dates) and all(stamp is not None and represented(stamp) for stamp in dates)
                if wrappers is not None and texts
                else None,
                "Aware retrieval timestamp and its actual date in the output",
            )
        else:
            record(key, None, "Frozen assertion is not implemented; no implicit pass")
    for metadata, owner in {
        "unit_kind": "exact_units",
        "time_anchor": "relative_due_seconds",
        "due_tolerance_seconds": "relative_due_seconds",
    }.items():
        if metadata in expected:
            result[metadata] = dict(
                result.get(owner, {"status": "NOT_RUN", "detail": "Owning assertion missing"})
            )
    statuses = {row["status"] for row in result.values()}
    overall = next(
        (status for status in ("FAIL", "NOT_RUN", "NEEDS_REVIEW") if status in statuses), "PASS"
    )
    return {
        "case_id": case["id"],
        "status": overall,
        "checks": result,
        "business_writes_applied_by_checker": False,
    }


_INSPECT = r"""
import hashlib, io, json, sys, wave, array
from pathlib import Path
from groupagent.artifacts.formats import inspect_office, open_pdf
raw = Path(sys.argv[1]).read_bytes()
fmt = sys.argv[2]
result = {"source_sha256": hashlib.sha256(raw).hexdigest(), "tables": [], "links": []}
if fmt == "md":
    result.update(text=raw.decode("utf-8"), units=[])
elif fmt == "docx":
    inspect_office(raw, "docx", for_edit=True)
    from docx import Document
    doc = Document(io.BytesIO(raw))
    pieces = []
    for block in doc.iter_inner_content():
        if hasattr(block, "rows"):
            rows = [[cell.text for cell in row.cells] for row in block.rows]
            result["tables"].append(rows)
            pieces.append("\n".join(" | ".join(row) for row in rows))
        else:
            pieces.append(block.text)
            for link in block.hyperlinks:
                if link.url: result["links"].append(link.url)
    for section in doc.sections:
        for story in (section.header, section.footer):
            pieces.extend(p.text for p in story.paragraphs)
    result.update(text="\n".join(pieces), units=[])
elif fmt == "pptx":
    inspect_office(raw, "pptx", for_edit=True)
    from pptx import Presentation
    deck = Presentation(io.BytesIO(raw)); units=[]
    for i, slide in enumerate(deck.slides, 1):
        texts=[]
        for shape in slide.shapes:
            if shape.has_text_frame: texts.append(shape.text)
            if shape.has_table:
                rows=[[cell.text for cell in row.cells] for row in shape.table.rows]
                result["tables"].append(rows)
                texts.extend(" | ".join(row) for row in rows)
        units.append({"index":i,"text":"\n".join(texts),"content_sha256":hashlib.sha256(slide._element.xml.encode()).hexdigest()})
    result.update(units=units,unit_count=len(units),text="\n".join(u["text"] for u in units))
elif fmt == "pdf":
    from groupagent.media.validation import ocr_pdf
    pdf = open_pdf(raw); pages = ocr_pdf(raw).pages
    units=[{"index":p.page_number,"text":p.text,"content_sha256":hashlib.sha256(p.text.encode()).hexdigest()} for p in pages]
    result.update(units=units,unit_count=len(pdf.pages),rendered_pages=len(pdf.pages),text="\n".join(p.text for p in pages))
elif fmt in ("png","jpeg","jpg","gif","webp"):
    from groupagent.media.validation import inspect_image, ocr_image
    from PIL import Image
    info,pixels=inspect_image(raw); raster=Image.open(io.BytesIO(pixels)).convert("L")
    extrema=raster.getextrema(); recognized=ocr_image(pixels)
    result["image"]={"decoded":True,"width":info.width,"height":info.height,"nonblank":extrema[1]-extrema[0]>5,"ocr_text":recognized.text,"static_first_frame":info.static_first_frame,"pixel_sha256":info.pixel_sha256}
elif fmt in ("wav","ogg","mp3","flac","m4a","aac"):
    from groupagent.media.validation import inspect_audio
    info,normalized=inspect_audio(raw, normalize=True)
    with wave.open(io.BytesIO(normalized),"rb") as stream: samples=array.array("h",stream.readframes(stream.getnframes()))
    if sys.byteorder!="little": samples.byteswap()
    result["audio"]={"decoded":True,"duration_seconds":info.duration_seconds,"non_silent":any(abs(value)>100 for value in samples)}
else:
    raise ValueError("Unsupported acceptance inspection format")
print(json.dumps(result,ensure_ascii=False,allow_nan=False))
"""


async def decode_artifact(worker: Any, context: Any, format: str, content: bytes) -> dict[str, Any]:
    """Inspect actual bytes inside the existing isolated trusted Python runner.

    It does not change or add a product worker operation. Returned audio still
    needs a separately supplied real ASR transcript/receipt for content checks.
    """
    from groupagent.domain import DomainError
    from groupagent.sandbox import ProcessSpec
    from groupagent.tools.paths import open_parent

    allowed = {
        "md",
        "docx",
        "pdf",
        "pptx",
        "png",
        "jpeg",
        "jpg",
        "gif",
        "webp",
        "wav",
        "ogg",
        "mp3",
        "flac",
        "m4a",
        "aac",
    }
    if format not in allowed or len(content) > 30 * 1024 * 1024:
        raise ValueError("Unsupported or oversized acceptance bytes")
    name = "acceptance-" + uuid4().hex + ".bin"
    worker._write(context, name, content)
    try:
        outcome = await worker.sandbox.run(
            ProcessSpec(
                program="python3",
                args=("-I", "-c", _INSPECT, name, format),
                input_paths=(name,),
                timeout_seconds=180,
                max_output_bytes=2_000_000,
            ),
            context,
        )
        if not outcome.succeeded:
            raise DomainError(
                "acceptance_decode_failed", "Isolated artifact inspection did not finish"
            )
        try:
            decoded = json.loads(outcome.stdout)
        except ValueError:
            raise DomainError(
                "acceptance_decode_failed", "Inspector returned no structured data"
            ) from None
        if (
            not isinstance(decoded, dict)
            or decoded.get("source_sha256") != hashlib.sha256(content).hexdigest()
        ):
            raise DomainError("acceptance_decode_failed", "Inspector described different bytes")
        return decoded
    finally:
        try:
            with open_parent(context.workspace, name) as (parent, leaf):
                try:
                    os.unlink(leaf, dir_fd=parent)
                except FileNotFoundError:
                    pass
        except DomainError:
            pass


def evaluate_interaction_participation(
    dataset: dict[str, Any], records: list[dict[str, Any]], *, expected_denominators=(219, 32, 13)
) -> dict[str, Any]:
    """Offline replay of the actual service gate, not model accuracy or business execution."""
    import inspect
    from collections import Counter
    from types import SimpleNamespace

    from groupagent.config import AppConfig, GroupConfig
    from groupagent.conversation.service import ConversationCoordinator
    from groupagent.coordinator import Decision
    from groupagent.domain import ActorContext, EventKind, GroupEvent, Role
    from groupagent.identity import BotIdentity

    cases = dataset["cases"]
    digest = hashlib.sha256(
        json.dumps(cases, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()
    if dataset.get("cases_sha256") != digest:
        raise ValueError("Frozen interaction labels changed")
    by_id = {row["id"]: row for row in records}
    if len(by_id) != len(records) or set(by_id) != {case["id"] for case in cases}:
        raise ValueError("Interaction record denominator is incomplete or duplicated")
    silent = {"observe", "ignore"}
    groups = {"explicit": [], "should_not_intervene": [], "other": []}
    for case in cases:
        kinds = set(case["expected_kind"])
        group = (
            "should_not_intervene"
            if kinds <= silent
            else "explicit"
            if not kinds & silent
            else "other"
        )
        groups[group].append(case)
    denominators = tuple(len(groups[key]) for key in groups)
    if denominators != tuple(expected_denominators):
        raise ValueError("Fixed participation denominators changed")

    class Cursor:
        def __init__(self, query):
            self.query = query

        def fetchall(self):
            return []

        def fetchone(self):
            if self.query.lstrip().startswith("SELECT count(*)"):
                return {"n": 0}
            if self.query.lstrip().startswith("SELECT epoch "):
                return {"epoch": 1}
            if self.query.lstrip().startswith("SELECT sequence "):
                return {"sequence": 1}
            return None

    class ReadOnlyPolicies:
        def __init__(self, group_id):
            self.group_id = group_id

        def execute(self, query, params=()):
            # Simulate the real gate's snapshot lock/epoch initialization only;
            # this fixture has no database or mutable business resources.
            if query.startswith(
                (
                    "SELECT pg_advisory_xact_lock(",
                    "INSERT INTO ga_memory_epochs(group_id) VALUES (%s) ON CONFLICT DO NOTHING",
                )
            ):
                return Cursor(query)
            if query == (
                "SELECT member_id,COALESCE(room_name,confirmed_name,'群昵称待补充') AS name,"
                "CASE WHEN room_name IS NOT NULL THEN 'room_display_name' "
                "WHEN confirmed_name IS NOT NULL THEN 'owner_confirmed' ELSE 'unknown' END AS name_source,"
                "membership_state FROM ga_members "
                "WHERE group_id=%s AND membership_state<>'left' ORDER BY member_id LIMIT 10000"
            ) and params == (self.group_id,):
                # Only the recipient resolver's bounded roster projection for
                # this fixture group. Other roster reads/writes still fail.
                return Cursor(query)
            if re.search(r"\bga_members\b", query, re.I):
                raise AssertionError(
                    "Offline roster reads require the exact current-group projection"
                )
            if not query.lstrip().startswith("SELECT") or not any(
                table in query
                for table in (
                    "ga_conversation_policy",
                    "ga_participation_log",
                    "ga_memory_epochs",
                    "ga_events",
                )
            ):
                raise AssertionError(
                    "Offline gate evaluation cannot write or access other resources"
                )
            return Cursor(query)

    outcomes = {}
    missing = []
    for case in cases:
        row = by_id[case["id"]]
        if row.get("labels_sha256") != digest:
            raise ValueError("Recorded model output used different frozen labels")
        if not isinstance(row.get("decision"), dict):
            missing.append(case["id"])
            continue
        context = case["context"]
        role = Role(case["role"])
        actor = ActorContext(
            group_id=context["group_id"], member_id=context["requester_member_id"], role=role
        )
        stamp = _date(case.get("occurred_at") or context.get("now"))
        if stamp is None:
            raise ValueError("Canonical frozen event timestamp is missing")
        projection = context.get("model_event", {})
        event = GroupEvent(
            id=projection.get("id", case["id"]),
            source_key="offline-gate:" + case["id"],
            group_id=actor.group_id,
            actor_id=actor.member_id,
            kind=EventKind(case.get("event_kind", "text")),
            text=case["text"],
            occurred_at=stamp,
            received_at=stamp,
            reply_to=projection.get("reply_to"),
            attachments=projection.get("attachments", []),
        )
        coordinator = ConversationCoordinator.__new__(ConversationCoordinator)
        coordinator.config = AppConfig(
            group=GroupConfig(
                enabled=True,
                internal_id=actor.group_id,
                external_group_id="offline@chatroom",
                owner_ids=[actor.member_id if role == Role.OWNER else "offline-owner"],
            )
        )
        coordinator.clock = lambda instant=stamp: instant
        decision = Decision.model_validate(row["decision"])
        coordinator.identity = BotIdentity.from_context(context)
        effective = coordinator._gate(
            SimpleNamespace(
                actor=actor,
                group_id=actor.group_id,
                conn=ReadOnlyPolicies(actor.group_id),
                is_bot_message=lambda _message_id: False,
            ),
            event,
            decision,
            context,
        )
        outcomes[case["id"]] = {
            "kind": effective.kind,
            "reason_code": effective.reason_code,
            "participated": effective.kind not in silent,
            "raw_kind": decision.kind,
            "raw_directness": decision.directness,
        }
    explicit = [case["id"] for case in groups["explicit"]]
    negative = [case["id"] for case in groups["should_not_intervene"]]
    others = [case["id"] for case in groups["other"]]
    answered = [key for key in explicit if outcomes.get(key, {}).get("participated")]
    false_interventions = [key for key in negative if outcomes.get(key, {}).get("participated")]
    unavailable = {
        key: [case["id"] for case in values if case["id"] in missing]
        for key, values in groups.items()
    }
    return {
        "dataset_sha256": digest,
        "source_gate_sha256": hashlib.sha256(
            inspect.getsource(ConversationCoordinator._gate).encode()
        ).hexdigest(),
        "explicit_request_recall": {
            "numerator": len(answered),
            "denominator": len(explicit),
            "rate": len(answered) / len(explicit),
            "unavailable_decisions": len(unavailable["explicit"]),
            "lower_bound": len(answered) / len(explicit),
            "upper_bound": (len(answered) + len(unavailable["explicit"])) / len(explicit),
            "misses": [key for key in explicit if key not in answered],
        },
        "should_not_intervene_rate": {
            "numerator": len(false_interventions),
            "denominator": len(negative),
            "rate": len(false_interventions) / len(negative),
            "unavailable_decisions": len(unavailable["should_not_intervene"]),
            "lower_bound": len(false_interventions) / len(negative),
            "upper_bound": (len(false_interventions) + len(unavailable["should_not_intervene"]))
            / len(negative),
            "false_interventions": false_interventions,
        },
        "other_13": {
            "denominator": len(others),
            "outcomes": dict(
                Counter(outcomes.get(key, {}).get("kind", "NOT_RUN") for key in others)
            ),
            "case_ids": others,
        },
        "missing_decisions": missing,
        "missing_by_denominator": unavailable,
        "rate_interpretation": "Missing provider decisions remain in the fixed denominators; rate is confirmed lower bound, not an assumed observed success/silence.",
        "complete_evidence": not missing,
        "effective": outcomes,
        "business_writes_applied": 0,
        "provider_calls": 0,
        "assumptions": [
            "Actual service._gate replay under default natural mode; no persisted quiet/group overrides; independent-window proactive count is zero.",
            "Only participation is measured. Correct action/target/arguments and transactional effects are separate.",
            "Frozen synthetic labels are not human adjudication or an unseen holdout.",
        ],
    }
