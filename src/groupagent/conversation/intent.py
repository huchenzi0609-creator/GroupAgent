"""Conservative lexical guard for an explicit request to perform actual work.

This is a completion-evidence backstop, not the primary natural-language router.
It matches the main request verb after address/politeness, not tool words that
occur inside questions, translations or examples.
"""

from __future__ import annotations

import re


def main_request(text: str) -> str:
    text = re.sub(r"^\s*(?:/组长|@?机器人组长|@?组长)[，,:：\s]*", "", text).strip()
    previous = None
    while text != previous:
        previous = text
        text = re.sub(
            r"^(?:请你?|麻烦你?|帮我|替我|能否|能不能|可不可以|(?:你)?可以)[，,：:\s]*", "", text
        ).strip()
    return text


def explicitly_requires_execution(text: str) -> bool:
    main = main_request(text)
    # Quoted strings are request data, not request verbs.
    main = re.sub(r"“[^”]*”|『[^』]*』|「[^」]*」|\"[^\"]*\"", "<quoted data>", main)
    if re.match(r"(?:解释|说明|说说|翻译|为什么|如何|怎么|如果|假如|假设|不要|不用|别|不想)", main):
        return False
    if re.match(
        r"(?:真正|实际|现在|直接)?(?:用|使用)(?:受控的?)?\s*(?:Python|Shell|python|shell)", main
    ):
        return not bool(re.search(r"(?:为什么|如何|怎么|会不会|是什么意思|有哪些步骤)", main))
    if re.match(
        r"(?:真正|实际|现在|直接)?(?:运行|执行).{0,20}(?:Python|Shell|python|shell|代码|进程)", main
    ):
        return True
    return bool(
        re.match(
            r"(?:(?:生成|制作|导出|交付)|把.{0,35}(?:整理|转换|导出|生成)成).{0,35}(?:文件|PDF|pdf|Word|docx|PPT|pptx|Markdown|markdown|文档|图表|图片|语音)",
            main,
        )
    )


def task_write_request(text: str) -> bool:
    outside = re.sub(
        r"“[^”]*”|『[^』]*』|「[^」]*」|\"[^\"]*\"", "<quoted data>", main_request(text)
    )
    if re.search(
        r"(?:不要|不用|别|不作|不做).{0,8}(?:登记|分配|任务|落库|写入)|仅供参考|只.{0,5}(?:建议|方案)|虚构分工",
        outside,
    ):
        return False
    return bool(
        re.search(
            r"登记.{0,10}(?:任务|分工|进度)|(?:任务|分工|进度).{0,10}(?:落库|写入|保存|登记)|(?:给|为).{0,12}分工|分配.{0,12}任务|更新.{0,12}(?:任务|进度)|标记.{0,10}完成",
            outside,
        )
    )


_RESEARCH_ACTION = r"(?:(?:联网|上网)(?:调查|研究|搜索|检索|查询)?|搜索|检索)(?:过|了)?"
_NEGATIVE_RESEARCH = re.compile(
    r"(?:不要|不得|禁止|不用|不必|无需|不准|别)\s*(?:再|去|进行)?\s*"
    + _RESEARCH_ACTION
    + r"|(?:不要|不得|禁止|别)\s*(?:声称|宣称|谎称|假装|说)"
    r"(?:(?:自己|已经|曾经|做过|做了|进行过|进行|实际|真正|你|我|已|曾|过|了)\s*){0,6}"
    + _RESEARCH_ACTION
    + r"|(?:没有|未曾|尚未|未)(?:(?:做过|做了|进行过|进行|任何)\s*){0,3}"
    + _RESEARCH_ACTION
    + r"|(?:不要|不得|禁止|不用|别)\s*(?:读取|打开|查看|访问)\s*https?://[^\s，。；;!?！？]+"
)
_POSITIVE_RESEARCH = re.compile(
    r"联网|上网|搜索|检索|找.{0,12}(?:文献|来源|资料|报道)|最新.{0,10}(?:资料|新闻|政策)"
    r"|查阅.{0,12}(?:网页|官网|文献)|https?://|search|web\s*(?:page|research)|fetch",
    re.I,
)


def requires_research(text: str) -> bool:
    """Ignore only explicit no-network/no-research-claim spans, not other clauses.

    This is a narrow completion-requirement guard, not a new authority source.
    A separate positive request or actual URL still requires real tool evidence.
    """
    return bool(_POSITIVE_RESEARCH.search(_NEGATIVE_RESEARCH.sub(" ", text)))
