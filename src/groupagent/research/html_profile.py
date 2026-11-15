"""Static HTML profile: no scripts, video/audio, frames, remote resources or forms."""

from __future__ import annotations

import base64
import html
import re
from html.parser import HTMLParser

from groupagent.domain import DomainError


class StaticHTML(HTMLParser):
    blocked = {
        "script",
        "video",
        "audio",
        "iframe",
        "object",
        "embed",
        "source",
        "track",
        "link",
        "meta",
        "base",
        "bgsound",
        "foreignobject",
    }
    void = {"embed", "source", "track", "link", "meta", "base", "bgsound"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.suppressed: list[str] = []
        self.style = False
        self.removed = 0

    @staticmethod
    def safe_style(value: str) -> bool:
        return not re.search(
            r"url\s*\(|@import|expression\s*\(|behavior\s*:|animation", value, re.IGNORECASE
        )

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in self.blocked:
            self.removed += 1
            if tag not in self.void:
                self.suppressed.append(tag)
            return
        if self.suppressed:
            return
        attributes = dict(attrs)
        kept = []
        for name, value in attrs:
            if value is None:
                continue
            if name in {
                "id",
                "class",
                "title",
                "alt",
                "width",
                "height",
                "colspan",
                "rowspan",
                "viewbox",
                "d",
                "fill",
                "stroke",
                "x",
                "y",
                "cx",
                "cy",
                "r",
            }:
                kept.append((name, value))
            elif name == "style" and self.safe_style(value):
                kept.append((name, value))
        if tag == "img":
            src = attributes.get("src") or ""
            match = re.fullmatch(r"data:image/(?:png|jpeg|gif|webp);base64,([A-Za-z0-9+/=]+)", src)
            if not match:
                self.parts.append(
                    "<span>"
                    + html.escape(attributes.get("alt") or "[image not fetched]")
                    + "</span>"
                )
                self.removed += 1
                return
            from groupagent.media.validation import inspect_image

            _, pixels = inspect_image(base64.b64decode(match[1], validate=True))
            kept.append(("src", "data:image/png;base64," + base64.b64encode(pixels).decode()))
        self.parts.append(
            "<"
            + tag
            + "".join(f' {name}="{html.escape(value, quote=True)}"' for name, value in kept)
            + ">"
        )
        self.style = tag == "style"

    def handle_endtag(self, tag: str) -> None:
        if self.suppressed:
            if tag == self.suppressed[-1]:
                self.suppressed.pop()
            return
        if tag not in self.blocked:
            self.parts.append("</" + tag + ">")
        if tag == "style":
            self.style = False

    def handle_data(self, data: str) -> None:
        if self.suppressed:
            return
        if self.style:
            if self.safe_style(data):
                self.parts.append(data)
            else:
                self.removed += 1
        else:
            self.parts.append(html.escape(data))


def static_html(content: bytes) -> tuple[str, int]:
    encoding = "utf-8"
    declared = re.search(rb"charset\s*=\s*[\"']?([a-zA-Z0-9_-]+)", content[:8192], re.IGNORECASE)
    if declared:
        candidate = declared[1].decode().lower()
        if candidate not in {
            "utf-8",
            "utf8",
            "gbk",
            "gb2312",
            "gb18030",
            "big5",
            "windows-1252",
            "iso-8859-1",
        }:
            raise DomainError(
                "browser_encoding_unsupported", "Page encoding is outside the declared profile"
            )
        encoding = candidate
    try:
        text = content.decode(encoding)
    except UnicodeError:
        raise DomainError(
            "browser_encoding_unsupported", "Page cannot be decoded without replacement"
        ) from None
    parser = StaticHTML()
    parser.feed(text)
    parser.close()
    return "<!doctype html><meta charset=utf-8>" + "".join(parser.parts), parser.removed
