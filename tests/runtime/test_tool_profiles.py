"""Capability guidance follows the admitted tool set, never request keywords."""

from groupagent.runtime.tool_profiles import build_tool_profile, instructions_for_tools


def test_web_profile_is_small_and_contains_only_selected_capability_guidance():
    selected = {"request_input", "web_search", "web_fetch", "web_read"}
    profile = build_tool_profile(selected)
    text = "\n".join(profile.instructions)
    assert profile.families == ("research",) and profile.tool_names == selected
    assert "web_read" in text and "source.citation" in text
    assert not any(term in text for term in ("PPT", "DOCX", "image_analyze", "Pillow", "ASR"))
    assert len(text.encode()) < 1641  # Over 80% below the former 8,207-byte instruction list.


def test_composed_profile_deduplicates_contributions_and_keeps_deterministic_order():
    selected = {"web_search", "web_fetch", "document_generate", "document_edit", "chart_render"}
    first = build_tool_profile(selected)
    second = build_tool_profile(reversed(sorted(selected)))
    assert first == second
    assert first.families == ("research", "documents", "compute")
    assert len(first.instructions) == len(set(first.instructions))
    assert instructions_for_tools(selected) == first.instructions


def test_empty_profile_contains_no_unavailable_tool_workflows():
    profile = build_tool_profile([])
    assert profile.tool_names == frozenset() and profile.families == ()
    assert "web_fetch" not in "\n".join(profile.instructions)
