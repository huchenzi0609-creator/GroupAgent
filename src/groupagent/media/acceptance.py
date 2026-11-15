"""Opt-in synthetic semantic acceptance. Importing never reads keys or calls APIs.

The caller supplies its already configured MediaService, shared monetary ledger
and trusted context. Speech cases are synthesized speech, not noisy real recordings.
"""

from __future__ import annotations

import io
import json
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from typing import Any

from PIL import Image, ImageDraw

from groupagent.artifacts import ChartData, ChartSeries
from groupagent.artifacts.visuals import _font, chart_png
from groupagent.domain import DomainError
from groupagent.tools import ToolContext

from .models import MediaSource, ProviderRoute
from .service import MediaService


@dataclass(frozen=True)
class ImageCase:
    case_id: str
    category: str
    question: str
    expected: tuple[str, ...]
    pixels: bytes = field(repr=False)


@dataclass(frozen=True)
class VoiceCase:
    case_id: str
    utterance: str
    expected_groups: tuple[tuple[str, ...], ...]
    critical_names: tuple[str, ...] = ()


def _png(image: Image.Image) -> bytes:
    output = io.BytesIO()
    image.save(output, "PNG")
    return output.getvalue()


def make_image_cases() -> tuple[ImageCase, ...]:
    cases = []
    for index in range(5):
        image = Image.new("RGB", (1200, 720), "white")
        draw = ImageDraw.Draw(image)
        code = f"COURSE-{17 + index}"
        draw.multiline_text(
            (90, 100),
            f"SYNTHETIC COURSE REPORT\nReport code: {code}\nSample count: {24 + index}",
            fill="#153044",
            font=_font(48),
            spacing=50,
        )
        cases.append(
            ImageCase(
                f"text-{index}",
                "printed_text",
                "图中的报告代码是什么？仅给出代码。",
                (code,),
                _png(image),
            )
        )
    for index in range(5):
        image = Image.new("RGB", (1200, 720), "white")
        draw = ImageDraw.Draw(image)
        draw.text((80, 35), "CLASSROOM RESULTS", fill="#153044", font=_font(44))
        values = (12 + index, 24 + index, 36 + index)
        for row, pair in enumerate(
            (
                ("Group", "Participants"),
                ("Alpha", str(values[0])),
                ("Beta", str(values[1])),
                ("Gamma", str(values[2])),
            )
        ):
            y = 140 + row * 115
            draw.rectangle(
                (80, y, 1120, y + 115),
                fill="#e8eff5" if row == 0 else "white",
                outline="#9daeba",
                width=2,
            )
            draw.line((600, y, 600, y + 115), fill="#9daeba", width=2)
            draw.text((110, y + 30), pair[0], fill="black", font=_font(38))
            draw.text((650, y + 30), pair[1], fill="black", font=_font(38))
        cases.append(
            ImageCase(
                f"table-{index}",
                "table",
                "Beta组有多少人？仅给出阿拉伯数字。",
                (str(values[1]),),
                _png(image),
            )
        )
    for kind in ("bar", "line"):
        for index in range(5):
            values = (
                (3 + index, 7 + index, 5 + index)
                if kind == "bar"
                else (2 + index, 4 + index, 8 + index)
            )
            chart = ChartData(
                kind,
                "SYNTHETIC STUDY DATA",
                ("Alpha", "Beta", "Gamma"),
                (ChartSeries("Score", values),),
            )
            category = "Beta" if kind == "bar" else "Gamma"
            answer = values[1] if kind == "bar" else values[2]
            cases.append(
                ImageCase(
                    f"{kind}-{index}",
                    kind + "_chart",
                    f"图中{category}的数值是多少？仅给出阿拉伯数字。",
                    (str(answer),),
                    chart_png(chart),
                )
            )
    locations = ("left", "right", "above", "below", "inside")
    aliases = (
        ("left", "左侧", "左边"),
        ("right", "右侧", "右边"),
        ("above", "上方"),
        ("below", "下方"),
        ("inside", "内部", "里面"),
    )
    for index, _location in enumerate(locations):
        image = Image.new("RGB", (1200, 720), "white")
        draw = ImageDraw.Draw(image)
        draw.rectangle((450, 260, 750, 560), outline="#225fba", width=14)
        center = ((250, 410), (950, 410), (600, 120), (600, 650), (600, 410))[index]
        draw.ellipse(
            (center[0] - 48, center[1] - 48, center[0] + 48, center[1] + 48), fill="#c92f36"
        )
        cases.append(
            ImageCase(
                f"position-{index}",
                "spatial_relation",
                "红色圆形相对蓝色正方形在哪里？答案选择left、right、above、below或inside。",
                aliases[index],
                _png(image),
            )
        )
    for index in range(5):
        image = Image.new("RGB", (1200, 720), "white")
        draw = ImageDraw.Draw(image)
        count = index + 2
        for circle in range(count):
            x, y = 180 + circle % 3 * 300, 210 + circle // 3 * 280
            draw.ellipse((x - 55, y - 55, x + 55, y + 55), fill="#225fba")
        draw.rectangle((1020, 100, 1110, 190), fill="#c92f36")
        cases.append(
            ImageCase(
                f"count-{index}",
                "object_count",
                "图中蓝色圆形有几个？不要数红色方块。仅给出阿拉伯数字。",
                (str(count),),
                _png(image),
            )
        )
    return tuple(cases)


def voice_cases() -> tuple[VoiceCase, ...]:
    values = (
        ("请把实验报告整理成三页摘要。", (("实验报告",), ("三页", "3页"), ("摘要",)), ()),
        ("小林负责收集资料，我来制作表格。", (("小林",), ("收集资料",), ("表格",)), ("小林",)),
        ("请明晚八点提醒我检查附件。", (("明晚", "明天晚上"), ("八点", "8点"), ("检查附件",)), ()),
        ("把第二节改短，保留原来的表格。", (("第二节", "第2节"), ("改短",), ("表格",)), ()),
        ("这次研究共包括二十四名学生。", (("研究",), ("二十四", "24"), ("学生",)), ()),
        ("请先暂停这个任务，不要继续生成文件。", (("暂停",), ("任务",), ("文件",)), ()),
        ("陈安会在周五提交初稿。", (("陈安",), ("周五", "星期五"), ("初稿",)), ("陈安",)),
        ("请把刚才那张图里的成绩读出来。", (("那张图",), ("成绩",), ("读出来",)), ()),
        (
            "我们下周一下午三点讨论课程项目。",
            (("下周一", "下星期一"), ("下午三点", "下午3点"), ("课程项目",)),
            (),
        ),
        (
            "报告需要包含两张图和一张表。",
            (("报告",), ("两张图", "2张图", "二张图"), ("一张表", "1张表")),
            (),
        ),
        ("请把结果保存成可以下载的文档。", (("结果",), ("下载",), ("文档",)), ()),
        (
            "周宁负责演示稿，李明负责检查数字。",
            (("周宁",), ("演示稿",), ("李明",), ("数字",)),
            ("周宁", "李明"),
        ),
        (
            "这份表格的正确率是百分之八十七。",
            (("表格",), ("正确率",), ("百分之八十七", "87%", "百分之87")),
            (),
        ),
        ("请取消明天早上的提醒。", (("取消",), ("明天早上",), ("提醒",)), ()),
        ("先比较这两个方案，再写结论。", (("比较",), ("两个方案", "2个方案"), ("结论",)), ()),
        (
            "请把第一页和第三页合并为一个新文件。",
            (("第一页", "第1页"), ("第三页", "第3页"), ("新文件",)),
            (),
        ),
        ("现在可以继续制作课程报告了。", (("继续",), ("课程报告",)), ()),
        ("请用简短的语音朗读这段说明。", (("简短",), ("语音",), ("朗读",)), ()),
        ("我负责最后的格式检查。", (("我负责",), ("格式检查",)), ()),
        ("请列出资料来源和访问日期。", (("资料来源",), ("访问日期",)), ()),
    )
    return tuple(
        VoiceCase(f"speech-{index:02d}", text, groups, names)
        for index, (text, groups, names) in enumerate(values)
    )


def _normal(value: str) -> str:
    return re.sub(r"[\s，。！？,.!?：:;；\"']", "", value).casefold()


async def run_live_acceptance(
    service: MediaService,
    base_context: ToolContext,
    *,
    vision_route: ProviderRoute,
    tts_route: ProviderRoute,
    asr_route: ProviderRoute,
    run_id: str,
    image_limit: int = 30,
    voice_limit: int = 20,
    on_case: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
) -> dict[str, Any]:
    """Explicitly called by the budget-owning parent; never discovers credentials."""
    if (
        not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", run_id)
        or not 0 <= image_limit <= 30
        or not 0 <= voice_limit <= 20
    ):
        raise ValueError("A bounded acceptance run ID and sample limits are required")
    records = []
    for case in make_image_cases()[:image_limit]:
        context = replace(
            base_context,
            call_id=run_id + ":" + case.case_id,
            idempotency_key=run_id + ":" + case.case_id,
        )
        source = MediaSource(
            group_id=context.group_id,
            work_id=context.work_id,
            member_id=context.actor.member_id,
            source_event_id=f"synthetic:{run_id}:{case.case_id}",
            source_message_id=f"synthetic:{case.case_id}",
            attachment_id=case.case_id,
            declared_kind="image",
            origin_kind="synthetic_fixture",
        )
        record: dict[str, Any] = {
            "case_id": case.case_id,
            "category": case.category,
            "expected": case.expected,
            "layer": "live_provider_synthetic_image",
        }
        try:
            reference = await service.ingest_image(context, source, case.pixels)
            result = await service.analyze_images(
                context,
                (reference,),
                '只返回一个JSON对象，格式为{"answer":"答案"}。' + case.question,
                vision_route,
            )
            text = result.text.strip()
            if text.startswith("```"):
                text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text)
            answer = json.loads(text)["answer"]
            passed = isinstance(answer, (str, int, float)) and _normal(str(answer)) in {
                _normal(value) for value in case.expected
            }
            record.update(
                status="PASS" if passed else "FAIL",
                answer=answer,
                raw_response=result.text,
                input_file_id=reference.file_id,
                input_version_id=reference.version_id,
                input_sha256=reference.sha256,
                provider_request_ids=[result.request_id],
                budget_receipts=result.budget_receipts,
            )
        except (DomainError, ValueError, KeyError, TypeError) as error:
            record.update(
                status="FAIL",
                error_code=getattr(error, "code", "answer_schema_invalid"),
                budget_receipt=getattr(error, "resource_ref", None),
            )
        records.append(record)
        if on_case:
            await on_case(record)
    for voice in voice_cases()[:voice_limit]:
        context = replace(
            base_context,
            call_id=run_id + ":" + voice.case_id,
            idempotency_key=run_id + ":" + voice.case_id,
        )
        source = MediaSource(
            group_id=context.group_id,
            work_id=context.work_id,
            member_id=context.actor.member_id,
            source_event_id=f"synthetic:{run_id}:{voice.case_id}",
            source_message_id=f"synthetic:{voice.case_id}",
            attachment_id=voice.case_id,
            declared_kind="text",
            origin_kind="synthetic_fixture",
        )
        record = {
            "case_id": voice.case_id,
            "utterance": voice.utterance,
            "expected_groups": voice.expected_groups,
            "layer": "live_provider_synthesized_speech",
            "real_noisy_recording": False,
        }
        try:
            audio = await service.synthesize(
                context, voice.utterance, tts_route, source, explicitly_requested=True
            )
            record.update(
                audio_file_id=audio.file_id,
                audio_version_id=audio.version_id,
                audio_sha256=audio.sha256,
                tts_budget_receipts=audio.budget_receipts,
            )
            transcript = await service.transcribe(
                context, audio, asr_route, critical_terms=voice.critical_names
            )
            normalized = _normal(transcript.text)
            passed = all(
                any(_normal(term) in normalized for term in alternatives)
                for alternatives in voice.expected_groups
            )
            record.update(
                status="PASS" if passed else "FAIL",
                transcript=transcript.text,
                needs_clarification=transcript.needs_clarification,
                confidence=transcript.confidence,
                provider_request_ids=transcript.request_ids,
                asr_budget_receipts=transcript.budget_receipts,
            )
        except DomainError as error:
            record.update(status="FAIL", error_code=error.code, budget_receipt=error.resource_ref)
        records.append(record)
        if on_case:
            await on_case(record)
    images = [record for record in records if record["layer"] == "live_provider_synthetic_image"]
    voices = [record for record in records if record["layer"] == "live_provider_synthesized_speech"]
    return {
        "run_id": run_id,
        "image_required": 30,
        "image_attempted": len(images),
        "image_passed": sum(record["status"] == "PASS" for record in images),
        "voice_required": 20,
        "voice_attempted": len(voices),
        "voice_passed": sum(record["status"] == "PASS" for record in voices),
        "image_generation_live": "NOT_APPLICABLE_user_scope_change",
        "records": records,
        "limitations": [
            "Synthetic image golds and synthesized speech do not establish real-group or noisy-recording acceptance",
            "The caller owns the shared monetary budget and persistence of these evidence records",
        ],
    }
