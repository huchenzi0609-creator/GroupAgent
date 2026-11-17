"""Shared group-facing delivery defaults, distinct from internal evidence."""

from __future__ import annotations


def group_reply_instruction(short_reply_style: str = "one_to_three_short_sentences") -> str:
    style = (
        "日常默认一到三个短句，不做字数硬截断。"
        if short_reply_style == "one_to_three_short_sentences"
        else "日常简短作答，不做字数硬截断。"
    )
    return (
        "群内回复用平实自然的中文和纯文本，直接回答本轮真人。"
        + style
        + "按真人要求给足必要事实；明确要求详细解释、逐项内容或文件时再展开。"
        "以本轮问题的最小足够范围作答；查到更多资料不等于用户要求更多分支。"
        "可操作的回答不必穷举所有平台、同名对象、背景和安装界面细节；仅保留完成当前问题必需的步骤、选择和限制。"
        "自动细化的goal和criteria用于安排内部执行，不自动要求群答案逐项复述；"
        "群内展示细节以真人请求及其后续真实变更为准。仍须完成当前授权目标、显式产物、引用、时间及提醒原文要求，"
        "不能用最初请求覆盖后来真人已改变的目标，也不能以简短为由省略这些实质要求。"
        "确实无法确定目标且会影响答案时才提出必要澄清，不附上其他对象的延伸介绍或未来服务邀约。"
        "默认不堆列来源清单或访问时间；需要时少量给出真实来源链接，真人明确要求的引用和时间必须保留。"
        "完整来源、实际检索时间和核验记录保存在内部证据中，不因回复简短而省略核验。"
        "收尾直接结束，不附加无必要的追问、邀约或‘需要我再……吗’；只有必要信息缺失或真实阻碍才聚焦澄清。"
    )


def group_delivery_policy(
    short_reply_style: str = "one_to_three_short_sentences",
) -> dict[str, str | list[str]]:
    """Project defaults; human requirements stay in their authoritative inputs.

    Whether a human requested citations, access times or detail is semantic,
    rather than a keyword-based rewrite of the original request.
    """
    return {
        "channel": "wechat_group",
        "short_reply_style": short_reply_style,
        "instruction": group_reply_instruction(short_reply_style),
        "requirement_sources": [
            "human_requirements.requests",
            "human_requirements.required_outputs",
            "human_requirements.reminder_body",
            "human_requirements.retained_authorized_context",
        ],
        "execution_plan_authority": "internal_execution_reference_only",
        "source_display_default": "minimal_when_useful",
        "retrieval_time_display_default": "internal_evidence_only",
    }
