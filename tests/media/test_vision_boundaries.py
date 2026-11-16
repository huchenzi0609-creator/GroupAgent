from dataclasses import replace

import pytest

from groupagent.media.tools import register_media_tools
from groupagent.tools import EffectState, ToolGateway
from groupagent.vision_policy import requests_face_identification


@pytest.mark.parametrize(
    "text,expected",
    [
        ("图片里这个男人是谁？", True),
        ("请确认这位男士的真实姓名。", True),
        ("Who is the man in this photo?", True),
        ("照片里是哪位明星？", True),
        ("描述这位男士的衣服颜色。", False),
        ("读取海报上写的姓名。", False),
        ("这位男士姓名由我提供为测试甲，把名字写在照片旁。", False),
        ("图片是谁发的？", False),
        ("这只卡通青蛙是谁？", False),
        ("不要识别这个人的身份，只描述衣着。", False),
        ("群里谁负责PPT？", False),
    ],
)
def test_visual_description_and_metadata_are_separate_from_person_identity(text, expected):
    assert requests_face_identification(text, image_context=True) is expected


async def test_face_identity_tool_is_denied_before_loading_images_or_calling_provider(
    media_context,
):
    gateway = ToolGateway()
    register_media_tools(gateway, object(), {})
    scope = replace(media_context.scope, allowed_tools=frozenset({"image_analyze"}))
    gateway.bind_scope(scope)
    result = await gateway.invoke(
        "image_analyze",
        {
            "images": [{"file_id": "synthetic-image"}],
            "route_id": "not-called",
            "prompt": "这张照片里的男人是谁？",
        },
        replace(media_context, scope=scope),
    )
    assert not result.success and result.error_code == "face_identification_unavailable"
    assert result.effect_state == EffectState.NO_EFFECT
