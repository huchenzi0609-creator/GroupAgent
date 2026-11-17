"""Synthetic examples for WeChat's semantic plain-text presentation boundary."""

import pytest

from groupagent.runtime.text import render_wechat_text, visible_text


def test_markdown_structure_becomes_readable_plain_text():
    markdown = (
        "# 调研结论\n\n"
        "**重点**与*补充*。\n\n"
        "- 第一点\n- 第二点\n\n"
        "3. 第三步\n4. 第四步\n\n"
        "| 项目 | 数量 |\n| :--- | ---: |\n| A | 2 |\n| B | 3 |\n\n"
        "[原始资料](https://example.test/a_b?q=1&x=2)"
    )
    assert render_wechat_text(markdown) == (
        "调研结论\n\n重点与补充。\n\n"
        "• 第一点\n• 第二点\n\n3. 第三步\n4. 第四步\n\n"
        "项目：A\n数量：2\n\n项目：B\n数量：3\n\n"
        "原始资料（https://example.test/a_b?q=1&x=2）"
    )


def test_code_urls_identifiers_and_math_remain_literal():
    code = 'snake_case = "**literal**"\n__init__ = r"a_b\\c"'
    markdown = (
        "## 示例\n\n```python\n" + code + "\n```\n\n"
        "运行 `tool_name(arg_one)`，保留 snake_case 和 __init__。\n"
        "https://example.test/a_*b*?x=a_b_c&v=%2F#part_two\n"
        "公式 $a_b + c_d$、\\(x * y*\\) 与 $$a_1 * b_2$$。\n\n"
        "$$\n# literal_math * y*\nx_y\n$$\n"
    )
    rendered = render_wechat_text(markdown)
    assert code in rendered
    assert "tool_name(arg_one)" in rendered
    assert "snake_case 和 __init__" in rendered
    assert "https://example.test/a_*b*?x=a_b_c&v=%2F#part_two" in rendered
    assert "$a_b + c_d$、\\(x * y*\\) 与 $$a_1 * b_2$$" in rendered
    assert "$$\n# literal_math * y*\nx_y\n$$" in rendered
    assert "```" not in rendered


@pytest.mark.parametrize(
    ("markdown", "plain"),
    [
        ("[**资料**](https://例子.test/路径_a?x=%2f)", "资料（https://例子.test/路径_a?x=%2f）"),
        ("<https://example.test/a_b>", "https://example.test/a_b"),
        ("**https://example.test/a_b**", "https://example.test/a_b"),
        ("[https://example.test/a_b](https://example.test/a_b)", "https://example.test/a_b"),
        (
            "![图表](https://example.test/chart_a.png)",
            "图片：图表（https://example.test/chart_a.png）",
        ),
        ("[报告][ref]\n\n[ref]: https://example.test/a_b", "报告（https://example.test/a_b）"),
        ("> 引用 **结论**", "引用：\n引用 结论"),
        ("原文 ~~旧值~~", "原文 （已划去：旧值）"),
        ("- 第一层\n  - 第二层", "• 第一层\n  • 第二层"),
        ("普通回答。\n\n第二段。\n", "普通回答。\n\n第二段。\n"),
    ],
)
def test_links_and_semantics_are_preserved(markdown, plain):
    assert render_wechat_text(markdown) == plain


def test_unicode_sanitizing_remains_separate_and_idempotent():
    source = "中文\x00\ud800 snake_case **重点**"
    assert visible_text(source) == r"中文\u0000\ud800 snake_case **重点**"
    assert visible_text(visible_text(source)) == visible_text(source)
    assert render_wechat_text(source) == r"中文\u0000\ud800 snake_case 重点"
