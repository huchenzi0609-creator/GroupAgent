"""Literal text boundaries shared by policy and conversation interpretation."""

from __future__ import annotations

import re
from collections.abc import Iterator

QUOTED = re.compile(
    r"```.*?```|~~~.*?~~~|`[^`\n]*`|『[^』]*』|「[^」]*」|“[^”]*”|‘[^’]*’|\"[^\"]*\"|'[^'\n]*'|(?m:^>[^\n]*)",
    re.S,
)
EXPLANATION = re.compile(
    r"^(?:请|麻烦(?:你)?|帮我)?\s*(?:解释|翻译|说明|分析|总结|评价|判断|比较|如何|怎么|为什么)|是什么意思|有什么(?:区别|不同)"
)


def outside_quotes(text: str) -> str:
    return QUOTED.sub(" ", text).strip()


_QUOTE_EDGES = "`\"'“”‘’「」『』"
_REPLY_BREAKS = re.compile(r"[:：,，。；;！!？?\n\r]+")
_REPLY_EDGES = ":：,，。；;！!？?.*_~\n\r" + _QUOTE_EDGES
_REPLY_TRIM = " \t" + _REPLY_EDGES


def has_wrapped_source_copy(source: str, body: str) -> bool:
    """Spot a delimited copy worth checking, without inferring the author's intent."""

    def wrapped(span: str) -> bool:
        for match in re.finditer(re.escape(span), body):
            before = body[: match.start()].rstrip(" \t" + _QUOTE_EDGES)
            after = body[match.end() :].lstrip(" \t" + _QUOTE_EDGES)
            if (
                (match.start() or match.end() != len(body))
                and (not before or before[-1] in _REPLY_EDGES)
                and (not after or after[0] in _REPLY_EDGES)
            ):
                return True
        return False

    if any(span and wrapped(span) for span in _quoted_spans(source)):
        return True
    tail = source.rstrip(_REPLY_TRIM)
    if any(len(span) >= 3 and tail.endswith(span) for span in _quoted_spans(body)):
        return True
    # Bare suffixes need a substantive supplied span and a surrounding text
    # segment. This keeps ordinary names and punctuation-only prose off the probe.
    pieces = [part.strip(_REPLY_TRIM) for part in _REPLY_BREAKS.split(body)]
    return len([part for part in pieces if part]) > 1 and any(
        len(part) >= 3 and tail.endswith(part) and wrapped(part) for part in pieces
    )


def _quoted_spans(text: str) -> Iterator[str]:
    return (match[0].strip(" \t\r\n>~" + _QUOTE_EDGES) for match in QUOTED.finditer(text))
