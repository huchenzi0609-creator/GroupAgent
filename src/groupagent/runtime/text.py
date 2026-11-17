"""Visible output text, separate from exact checkpoint/artifact evidence."""

import re
from functools import lru_cache

from markdown_it import MarkdownIt
from markdown_it.rules_block.state_block import StateBlock
from markdown_it.rules_inline.state_inline import StateInline
from markdown_it.tree import SyntaxTreeNode


def visible_text(text: str) -> str:
    """Escape only non-displayable NUL/surrogate code points; retain valid text exactly.

    This is an idempotent presentation boundary. It neither removes source data
    nor grants authority; original tool/model strings remain in the lossless
    checkpoint or artifact while text transports receive valid Unicode.
    """
    return re.sub(
        "[\ud800-\udfff]",
        lambda match: f"\\u{ord(match[0]):04x}",
        text.replace("\x00", "\\u0000"),
    )


_URL_START = re.compile(r"(?:https?|ftp)://|mailto:", re.I)
_DUNDER = re.compile(r"__[A-Za-z][A-Za-z0-9_]*__\b")
_URL_END = frozenset("\n\r\t <>\"'`。。，；！？【】《》“”‘’")


def _closing_literal(source: str, marker: str, start: int) -> int:
    """Find a delimiter that is not itself backslash escaped."""
    while (end := source.find(marker, start)) >= 0:
        prefix = source[:end]
        if (len(prefix) - len(prefix.rstrip("\\"))) % 2 == 0:
            return end
        start = end + len(marker)
    return -1


def _literal_inline(state: StateInline, silent: bool) -> bool:
    """Keep URLs, math and Python special names out of emphasis/escape parsing."""
    source, start, end = state.src, state.pos, -1
    if _URL_START.match(source, start):
        end = start
        brackets = {"(": 0, "[": 0, "{": 0}
        pairs = {")": "(", "]": "[", "}": "{"}
        while end < state.posMax and source[end] not in _URL_END:
            char = source[end]
            if char in brackets:
                brackets[char] += 1
            elif char in pairs:
                opening_bracket = pairs[char]
                if not brackets[opening_bracket]:
                    break
                brackets[opening_bracket] -= 1
            end += 1
        # A surrounding emphasis delimiter belongs to the document, not its URL.
        for delimiter in reversed(state.delimiters):
            marker = chr(delimiter.marker) * delimiter.length
            if delimiter.open and source[start:end].endswith(marker):
                end -= len(marker)
                break
    elif (match := _DUNDER.match(source, start)) is not None:
        end = match.end()
    else:
        opening = next(
            (marker for marker in ("\\(", "\\[", "$$", "$") if source.startswith(marker, start)),
            None,
        )
        if opening:
            closing = {"\\(": "\\)", "\\[": "\\]"}.get(opening, opening)
            found = _closing_literal(source, closing, start + len(opening))
            if found >= 0 and (opening != "$" or "\n" not in source[start:found]):
                end = found + len(closing)
    if end <= start:
        return False
    if not silent:
        state.push("wechat_literal", "", 0).content = source[start:end]
    state.pos = end
    return True


def _math_block(state: StateBlock, start: int, end: int, silent: bool) -> bool:
    """Preserve multiline math before Markdown interprets its lines as blocks."""
    if state.is_code_block(start):
        return False
    first = state.bMarks[start] + state.tShift[start]
    opening = state.src[first : state.eMarks[start]].strip()
    if opening not in {"$$", "\\["}:
        return False
    closing = "$$" if opening == "$$" else "\\]"
    last = start + 1
    while last < end:
        line_start = state.bMarks[last] + state.tShift[last]
        if state.src[line_start : state.eMarks[last]].strip() == closing:
            if silent:
                return True
            token = state.push("wechat_literal", "", 0)
            token.block, token.map = True, [start, last + 1]
            token.content = state.getLines(start, last + 1, state.blkIndent, False)
            state.line = last + 1
            return True
        last += 1
    return False


class _WechatMarkdown(MarkdownIt):
    def normalizeLink(self, link: str) -> str:
        # No HTML is emitted, so do not rewrite Unicode hosts/paths or percent escapes.
        return link

    def normalizeLinkText(self, link: str) -> str:
        return link


@lru_cache(maxsize=1)
def _wechat_parser() -> MarkdownIt:
    parser = _WechatMarkdown("commonmark", {"html": False}).enable(["table", "strikethrough"])
    parser.inline.ruler.before("text", "wechat_literal", _literal_inline)
    for char in "hHfFmM":
        parser.inline.add_terminator_char(char)
    parser.block.ruler.before("fence", "wechat_math", _math_block)
    return parser


def _inline(node: SyntaxTreeNode) -> str:
    if node.type in {"softbreak", "hardbreak"}:
        return "\n"
    if node.type in {"text", "code_inline", "wechat_literal"}:
        return node.content
    content = "".join(_inline(child) for child in node.children)
    if node.type in {"link", "image"}:
        destination = str(node.attrGet("href" if node.type == "link" else "src") or "")
        if node.type == "image":
            content = "图片：" + content
        return (
            content if content == destination or not destination else content + f"（{destination}）"
        )
    if node.type == "s":
        return f"（已划去：{content}）"
    return content


def _table(node: SyntaxTreeNode) -> str:
    headings: list[str] = []
    records: list[str] = []
    for section in node.children:
        for row in section.children:
            cells = ["".join(_inline(child) for child in cell.children) for cell in row.children]
            if section.type == "thead":
                headings = cells
            else:
                records.append(
                    "\n".join(
                        f"{headings[index] or f'列{index + 1}'}：{value}"
                        for index, value in enumerate(cells)
                    )
                )
    return "\n\n".join(records) if records else "；".join(headings)


def _blocks(node: SyntaxTreeNode, *, depth: int = 0) -> str:
    if node.type in {"paragraph", "heading", "inline"}:
        return _inline(node)
    if node.type in {"code_block", "fence", "wechat_literal"}:
        return node.content
    if node.type == "hr":
        return ""
    if node.type == "table":
        return _table(node)
    if node.type in {"bullet_list", "ordered_list"}:
        items = []
        start = int(node.attrGet("start") or 1)
        for index, item in enumerate(node.children):
            marker = f"{start + index}. " if node.type == "ordered_list" else "• "
            children = []
            for child in item.children:
                value = _blocks(child, depth=depth + 1)
                if child.type not in {"bullet_list", "ordered_list"}:
                    value = value.replace("\n", "\n" + "  " * (depth + 1))
                children.append(value)
            items.append("  " * depth + marker + "\n".join(children))
        return "\n".join(items)
    content = "\n\n".join(_blocks(child, depth=depth) for child in node.children)
    return "引用：\n" + content if node.type == "blockquote" else content


def render_wechat_text(text: str) -> str:
    """Render one complete message as plain text, before identity/chunk freezing.

    This is presentation only: it preserves link destinations, citations and literal
    code/math. The delivery intent remains the original immutable source. Rendered
    delivery parts must be persisted and reused rather than rendered a second time.
    """
    safe = visible_text(text)
    rendered = _blocks(SyntaxTreeNode(_wechat_parser().parse(safe)))
    # Ordinary text keeps its final newlines, including already queued plain replies.
    trailing = safe[len(safe.rstrip("\n")) :]
    return rendered.rstrip("\n") + trailing
