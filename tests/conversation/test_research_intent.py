"""Fixed narrow research requirements; no model/API calls."""

import pytest

from groupagent.conversation.intent import requires_research

NEGATIVE = [
    "生成Markdown数据说明，明确全部数字来自本条合成请求，没有联网调查。",
    "制作带代码块的Markdown，不要声称做过联网调查。",
    "只按附件写文档，不要联网。",
    "禁止联网调查，仅使用已给数字。",
    "不得声称已经进行过联网搜索。",
    "不用上网检索，也不要宣称已经联网。",
    "说明尚未进行联网调查，不编造外部来源。",
    "不要访问https://example.org，只整理给定数据。",
]
POSITIVE = [
    "不要凭记忆，请联网查官方文档。",
    "不要声称已联网；请实际检索官网后再总结。",
    "请联网搜索官方来源，不要声称做过无关联网调查。",
    "读取https://docs.python.org/3/library/functions.html并总结。",
    "请根据https://example.org的页面核对说明。",
    "先不要联网写草稿；之后请联网检索官网核对。",
    "找最新新闻来源并给出链接。",
]


@pytest.mark.parametrize("text", NEGATIVE)
def test_negative_research_or_false_claim_constraint_is_not_research(text):
    assert not requires_research(text)


@pytest.mark.parametrize("text", POSITIVE)
def test_independent_positive_request_and_real_urls_remain_research(text):
    assert requires_research(text)
