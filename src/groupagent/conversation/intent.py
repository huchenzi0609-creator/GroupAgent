"""Conservative lexical guard for an explicit request to perform actual work.

This is a completion-evidence backstop, not the primary natural-language router.
It matches the main request verb after address/politeness, not tool words that
occur inside questions, translations or examples.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


def main_request(text: str) -> str:
    text = re.sub(r"^\s*(?:/组长|@?机器人组长|@?组长)[，,:：\s]*", "", text).strip()
    previous = None
    while text != previous:
        previous = text
        text = re.sub(
            r"^(?:请你?|麻烦你?|帮我|替我|能否|能不能|可不可以|(?:你)?可以)[，,：:\s]*", "", text
        ).strip()
    return text


@dataclass(frozen=True)
class NewWorkIntent:
    outputs: tuple[dict[str, Any], ...]
    required_tool: str | None = None


_QUOTED_DATA = re.compile(r'```.*?```|“[^”]*”|『[^』]*』|「[^」]*」|"[^"\n]*"', re.S)
_NEW_WORK_EXCLUSIONS = re.compile(
    r"如果|假如|假设|要是|除非|倘若|只有|仅供参考|只是.{0,4}(?:引用|转发)|不代表.{0,8}(?:授权|请求|指令)|"
    r"(?:等|待).{0,20}(?:再|才)|.{0,12}同意.{0,8}(?:再|才)|先.{0,8}(?:只|仅).{0,8}(?:讨论|方案)|"
    r"(?:别|不(?:要|用|必)?|先不|先别|暂不)(?:做|动手)|口头|口答|"
    r"(?:不要|不用|不必|禁止|不使用|不).{0,12}(?:调用|使用).{0,12}(?:工具|程序|代码)|"
    r"(?:确认|批准|同意|回复).{0,8}(?:后|再)|"
    r"(?:不要|不用|先别|暂不|不必|无需|不是让你|别).{0,8}"
    r"(?:制作|生成|创建|新建|撰写|编写|整理|计算|执行|运行|帮我|代写)|"
    r"这份|那份|这项|这个|那个|上述|上面|下面|附件|截图|之前|上次|刚才|已有|现有|历史|"
    r"沿用|上一版|刚发|原稿|草稿|引用|转发|仅作|不是.{0,8}(?:指令|请求|授权)|"
    r"继续|接着|追加|修改|改成|改为|替换|续写|重做|重新|同样|"
    r"我(?:来|负责|认领|接手|承担)|由我|交给|分配|分工|安排|认领|负责|来做|"
    r"取消|停止|暂停|恢复|重试|提醒|定时|定期|每(?:天|周|月)|闹钟|"
    r"本项目|这个项目|我们|我(?:的|们的)|项目进展|群里(?:的|消息|资料|讨论)|资料中|"
    r"插画|文生图|视频|抽帧|音轨|"
    r"是什么意思|什么意思|怎么|如何|为什么|是否|会不会|有哪些步骤|[?？]"
)


def _page_count(value: str) -> int | None:
    if value.isascii() and value.isdigit():
        number = int(value)
    else:
        if not re.fullmatch(
            r"(?:[一二两三四五六七八九]百(?:[零〇][一二三四五六七八九]|[一二三四五六七八九]十[一二三四五六七八九]?)?"
            r"|[一二三四五六七八九]?十[一二三四五六七八九]?|[一二两三四五六七八九])",
            value,
        ):
            return None
        digits = {ch: n for n, ch in enumerate("零一二三四五六七八九")}
        digits.update({"〇": 0, "两": 2})
        total = pending = 0
        for ch in value:
            if ch in digits:
                pending = digits[ch]
            elif ch in {"十", "百"}:
                total += (pending or 1) * (10 if ch == "十" else 100)
                pending = 0
            else:
                return None
        number = total + pending
    return number if 1 <= number <= 200 else None


_PPT_NUMBER = r"(?:[0-9]{1,3}|[零〇一二两三四五六七八九十百]+)"
_PPT_LITERAL = (
    r'(?:“[^“”\r\n]{1,1000}”|「[^「」\r\n]{1,1000}」|『[^『』\r\n]{1,1000}』|"[^"\r\n]{1,1000}")'
)
_PPT_LITERAL_RE = re.compile(_PPT_LITERAL)
_PPT_SEPARATOR = re.compile(r"[\s，,。；;:：.]+")
_PPT_HEAD = re.compile(
    r"(?:现在|直接|实际|真正)?(?:制作|生成|做成|新建|创建)(?:并(?:发送|交付))?"
    r"(?:一(?:份|套))?(?:恰好)?(?P<count>" + _PPT_NUMBER + r")\s*页(?:的)?"
    r"(?:可编辑(?:的)?)?(?:PPTX?|幻灯片)(?:文件)?(?:并(?:发送|交付))?",
    re.I,
)
_PPT_TOPIC = re.compile(r"主题(?:为|是)?\s*(?P<topic>《[^《》\r\n]{1,180}》|" + _PPT_LITERAL + r")")
_PPT_PAGE = re.compile(
    r"第(?P<index>" + _PPT_NUMBER + r")\s*页(?:的)?\s*标题(?:为|是)?\s*"
    r"(?P<title>" + _PPT_LITERAL + r")\s*[，,:：]\s*"
    r"(?:写|正文(?:为|是)?|内容(?:为|是)?)\s*"
    r"(?P<body>" + _PPT_LITERAL + r"(?:\s*(?:[、，,]\s*)?" + _PPT_LITERAL + r")*)"
)
_PPT_CONSTRAINT = re.compile(
    r"(?:只|仅)(?:用|使用)(?:这些|上述|给定)(?:的)?(?:内容|文字)"
    r"|(?:不|不要|不用|无需)(?:生成|制作|添加|使用|需要)?(?:额外的?)?插画"
    r"|(?:不|不要|不用|无需)(?:联网|搜索|检索)"
)


def _enumerated_ppt_intent(main: str) -> NewWorkIntent | None:
    """Consume a complete current-request slide specification, without history."""
    head = _PPT_HEAD.match(main)
    count = _page_count(head["count"]) if head else None
    if head is None or count is None or count > 100:
        return None
    separator = _PPT_SEPARATOR.match(main, head.end())
    if separator is None:
        return None
    position = separator.end()
    topic = _PPT_TOPIC.match(main, position)
    if topic:
        if not topic["topic"][1:-1].strip():
            return None
        separator = _PPT_SEPARATOR.match(main, topic.end())
        if separator is None:
            return None
        position = separator.end()
    visible = []
    for index in range(1, count + 1):
        page = _PPT_PAGE.match(main, position)
        if page is None or _page_count(page["index"]) != index:
            return None
        title = page["title"][1:-1]
        body = [value[0][1:-1] for value in _PPT_LITERAL_RE.finditer(page["body"])]
        if not title.strip() or len(title) > 90 or not 1 <= len(body) <= 6:
            return None
        if any(not value.strip() for value in body):
            return None
        visible.extend([title, *body])
        position = page.end()
        if position < len(main):
            separator = _PPT_SEPARATOR.match(main, position)
            if separator is None:
                return None
            position = separator.end()
    # Only reviewed constraints may follow all declared pages. In particular,
    # a positive creation prefix must not swallow a control/action suffix.
    tail = [value for value in _PPT_SEPARATOR.split(main[position:]) if value]
    if any(_PPT_CONSTRAINT.fullmatch(value) is None for value in tail):
        return None
    visible = list(dict.fromkeys(visible))
    if len(visible) > 30:  # Existing OutputRequirement literal limit.
        return None
    return NewWorkIntent(({"format": "pptx", "exact_units": count, "expected_text": visible},))


def new_work_intent(text: str) -> NewWorkIntent | None:
    """A bounded, self-contained new task; this hint never grants authority.

    Existing files, discussion references, human assignments, schedules and
    uncertain constraints stay with the model. Keep the full original request
    as the eventual goal; this parser extracts only verifiable output constraints.
    """
    if not text.strip() or len(text) > 6000:
        return None
    main = re.sub(r"^\s*(?:@?机器人组长|@?机器人|@?组长|@?小伴)[，,:：\s]*", "", text)
    main = main_request(main)
    main = re.sub(
        r"^(?:新建|创建|新开|另开)(?:一(?:个|项|份))?(?:独立(?:的)?)?"
        r"(?:工作|任务)[，,:：\s]*",
        "",
        main,
    )
    main = main_request(main)
    enumerated = _enumerated_ppt_intent(main)
    if enumerated is not None:
        return enumerated
    outside = _QUOTED_DATA.sub(" <quoted data> ", main)
    if _NEW_WORK_EXCLUSIONS.search(outside):
        return None
    clauses = [part.strip() for part in re.split(r"[，,。；;\n—]+", outside) if part.strip()]
    if not clauses:
        return None
    # Parse the whole outer request, not just a positive prefix followed by
    # arbitrary prose. Unrecognized clauses must stay with the model.
    for clause in clauses[1:]:
        if not re.fullmatch(
            r"(?:整理资料并)?(?:交付|导出|提供|保存)(?:为|成|一份|一个)?"
            r"(?:Markdown|PDF|Word|docx|PPTX?|PNG|JPEG)(?:文件|文档|格式)?"
            r"|(?:介绍|讲解|关于|主题为|主题是|内容为|内容是)[A-Za-z0-9\u4e00-\u9fff _+.-]{2,40}"
            r"|标题(?:必须|要求)?(?:为|是|写成|使用)\s*<quoted data>"
            r"|(?:也)?(?:不要|不用|禁止|不得|无需)(?:实际|自动|再次|直接|对外)?"
            r"(?:联网|搜索|检索|发送(?:到群(?:里)?)?|发到群(?:里)?|外发)",
            clause,
            re.I,
        ):
            return None
    if not re.match(
        r"^(?:现在|直接|实际|真正)?(?:制作|生成|撰写|编写|整理|计算|用|使用|执行|运行)", outside
    ):
        return None
    compute = bool(
        re.match(
            r"^(?:现在|直接|实际|真正)?(?:(?:用|使用)(?:受控的?)?\s*(?:Python|Shell)"
            r"|(?:执行|运行).{0,20}(?:Python|Shell|代码|进程))",
            outside,
            re.I,
        )
    )
    numeric = bool(
        re.match(r"^计算", outside)
        and re.search(r"\d", outside)
        and re.search(
            r"[+*/×÷=]|\d\s*-\s*\d|加|减|乘|除|平方|立方|总和|求和|平均|中位|方差|阶乘|素数|质数",
            outside,
        )
    )
    numeric_body = re.sub(
        r"^(?:现在|直接|实际|真正)?(?:(?:用|使用)(?:受控的?)?\s*(?:Python|Shell)(?:实际|真正)?|(?:执行|运行)\s*(?:Python|Shell)(?:实际|真正)?)?计算",
        "",
        clauses[0],
        flags=re.I,
    )
    if compute or numeric:
        if not re.fullmatch(
            r"(?:[0-9+*/×÷=().\s-]|到|至|的|加|减|乘|除|平方|立方|总和|求和|平均|中位数|方差|阶乘|前|后|个|项|从|与|和|整数|小数|偶数|奇数|素数|质数)+",
            numeric_body,
        ):
            return None
    tool = None
    if compute or numeric:
        tail = re.sub(
            r"^(?:现在|直接|实际|真正)?(?:(?:用|使用)(?:受控的?)?\s*(?:Python|Shell)|(?:执行|运行).{0,20}(?:Python|Shell|代码|进程))",
            "",
            outside,
            flags=re.I,
        ).strip()
        if compute and not re.sub(r"实际|真正|代码|进程|[\s，,。:：]", "", tail):
            return None
        if re.search(
            r"(?:不要|不用|禁止|不使用).{0,6}(?:Python|Shell|程序|代码|工具)|只.{0,4}(?:口答|解释|步骤)",
            outside,
            re.I,
        ):
            return None
        tool = "process_run" if re.search(r"Shell|进程", outside, re.I) else "python_compute"
    formats = [
        format_name
        for pattern, format_name in (
            (r"Word|docx", "docx"),
            (r"PPTX?|幻灯片", "pptx"),
            (r"PDF", "pdf"),
            (r"Markdown|\.md\b", "md"),
            (r"\bPNG\b", "png"),
            (r"\bJPE?G\b", "jpeg"),
        )
        if re.search(pattern, outside, re.I)
    ]
    if len(formats) > 1 or not formats and not tool:
        return None
    head_formats = list(
        re.finditer(r"Word|docx|PPTX?|幻灯片|PDF|Markdown|PNG|JPEG|\.md\b", clauses[0], re.I)
    )
    if head_formats and not re.fullmatch(
        r"(?:文件|文档|格式|报告|幻灯片)?", clauses[0][head_formats[-1].end() :].strip()
    ):
        # Never delete a format from the middle and absorb an unparsed outer
        # instruction into the subject. A subject must precede the format or
        # use its own explicitly parsed clause.
        return None
    if not tool:
        # Incomplete unrelated history cannot supply missing task details. Only
        # synthetic material or an explicit introductory subject is independent.
        subject = re.search(
            r"(?:介绍|讲解|关于|主题(?:为|是)|内容(?:为|是))\s*([^，。；;\n]{2,80})", outside
        )
        named_subject = (
            re.sub(
                r"Word|docx|PPTX?|PDF|Markdown|PNG|JPEG|\.md|[\d\s页份的]",
                "",
                subject[1].replace("<quoted data>", ""),
                flags=re.I,
            )
            if subject
            else ""
        )
        named_subject = named_subject.replace("<quoted data>", "").strip()
        if not re.search(r"合成|虚构|示例|演示", outside) and len(named_subject) < 2:
            return None
        head = re.sub(r"^(?:现在|直接|实际|真正)?(?:制作|生成|撰写|编写|整理)", "", clauses[0])
        head = re.sub(r"Word|docx|PPTX?|幻灯片|PDF|Markdown|PNG|JPEG|\.md\b", "", head, flags=re.I)
        head = re.sub(r"([0-9]{1,4}|[零〇一二两三四五六七八九十百]+)\s*(?:页|张幻灯片)", "", head)
        head = re.sub(r"^(?:一份|一个|一张|一套|的|文件|文档)+", "", head).strip()
        head = re.sub(r"并(?:交付|导出|提供|保存)$", "", head).strip()
        if head and not re.fullmatch(
            r"(?:合成|虚构|示例|演示)(?:(?:调研|调查|研究|项目|实验|数据|测试|介绍|课程|教学)?(?:报告|提纲|说明|文档|图表))?[0-9]*"
            r"|(?:介绍|讲解|关于|主题为|主题是|内容为|内容是)[A-Za-z0-9\u4e00-\u9fff _+.-]{2,40}",
            head,
        ):
            return None
        if re.search(
            r"(?:介绍|讲解|关于|主题为|主题是|内容为|内容是).*(?:在|等|待|再|才|先|当|若|被|由|让|执行|基于|根据|会议结论|实验结果|实验数据)",
            outside,
        ):
            return None
    pages = list(
        re.finditer(
            r"([0-9]{1,4}|[零〇一二两三四五六七八九十百]+)\s*(?:页|张幻灯片|slides?)", outside, re.I
        )
    )
    if len(pages) > 1 or re.search(
        r"(?:至少|最多|不少于|不超过|大约|约).{0,5}(?:页|幻灯片)|(?:页|幻灯片).{0,3}(?:左右|以上|以内)|\d+\s*(?:到|至|[-~～])\s*\d+\s*页",
        outside,
    ):
        return None
    output: dict[str, Any] = {"format": formats[0] if formats else "text"}
    if pages:
        count = _page_count(pages[0][1])
        if count is None or output["format"] not in {"docx", "pdf", "pptx"}:
            return None
        output["exact_units"] = count
    titles = re.findall(
        r'(?:标题)(?:必须|要求)?(?:为|是|写成|使用)\s*[“"『「]([^”"』」\n]{1,1000})[”"』」]', main
    )
    if len(titles) > 1 or re.search(r"标题|逐字|原样|字符串", outside) and not titles:
        return None
    if titles:
        output["expected_text"] = titles
    return NewWorkIntent((output,), tool)


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
