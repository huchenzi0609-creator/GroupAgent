"""Visible output text, separate from exact checkpoint/artifact evidence."""

import re


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
