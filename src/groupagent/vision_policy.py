"""Keep image description/OCR separate from identifying real people by appearance."""

import re

FACE_ID_RESPONSE = "我不能根据照片确认真人身份；可以描述画面、衣着，或读取图中文字。"
_QUOTED = re.compile(r'```.*?```|“[^”]*”|「[^」]*」|『[^』]*』|"[^"\n]*"|‘[^’]*’', re.S)


def requests_face_identification(text: str, *, image_context: bool = False) -> bool:
    plain = _QUOTED.sub(" ", text)
    if re.search(r"谁(?:发|上传)|(?:发送者|上传者|消息作者)|who (?:sent|uploaded)", plain, re.I):
        return False
    plain = re.sub(
        r"(?:不要|不用|无需|别|不能|无法).{0,8}(?:识别|辨认|确认).{0,12}(?:身份|姓名|名字|是谁)",
        " ",
        plain,
    )
    if re.search(
        r"卡通|动漫|动画|虚构|游戏角色|吉祥物|fictional|cartoon|anime", plain, re.I
    ) and not re.search(r"真人|现实人物|照片|人脸|real person|face", plain, re.I):
        return False
    image_context = image_context or bool(
        re.search(r"图片|图中|图里|照片|相片|人脸|photo|picture|image|face", plain, re.I)
    )
    if not image_context:
        return False
    if re.search(
        r"(?:读取|读出|OCR|文字|写着|写的).{0,20}(?:名字|姓名|文字)|(?:名字|姓名).{0,12}(?:文字|写着|写的)",
        plain,
        re.I,
    ):
        return False
    person = bool(
        re.search(
            r"男人|女人|男士|女士|男生|女生|人物|这个人|这人|这位|人脸|真人|名人|演员|明星|群成员|男性|女性|\b(?:man|woman|person|face)\b",
            plain,
            re.I,
        )
    )
    identity = bool(
        re.search(
            r"是谁|哪位|哪名|认出|认得|叫什么|(?:识别|确认|辨认|判断|核对|找出|查出|告诉我).{0,24}(?:姓名|名字|身份|本人)|(?:姓名|名字|身份).{0,10}(?:是什么|是谁|叫什么)|是不是.{0,20}本人|\b(?:who|identify|recognize)\b|\bname\s+(?:this|that|the)\s+(?:person|man|woman)\b",
            plain,
            re.I,
        )
    )
    return (
        person
        and identity
        or bool(re.search(r"^(?:这|那|他|她)(?:个|位|人)?是谁[?？。\s]*$", plain))
    )
