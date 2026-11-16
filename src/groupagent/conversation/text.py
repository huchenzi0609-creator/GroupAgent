"""Literal text boundaries shared by policy and conversation interpretation."""

from __future__ import annotations

import re

QUOTED = re.compile(
    r"```.*?```|~~~.*?~~~|`[^`\n]*`|『[^』]*』|「[^」]*」|“[^”]*”|‘[^’]*’|\"[^\"]*\"|'[^'\n]*'|(?m:^>[^\n]*)",
    re.S,
)
EXPLANATION = re.compile(
    r"^(?:请|麻烦(?:你)?|帮我)?\s*(?:解释|翻译|说明|分析|总结|评价|判断|比较|如何|怎么|为什么)|是什么意思|有什么(?:区别|不同)"
)


def outside_quotes(text: str) -> str:
    return QUOTED.sub(" ", text).strip()
