"""Actual Chromium DOM rendering inside a network-disabled sandbox."""

from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

from groupagent.domain import DomainError

from .html_profile import static_html


async def _snapshot(content: bytes, output: Path) -> dict[str, Any]:
    from playwright.async_api import async_playwright

    chromium = shutil.which("chromium") or shutil.which("chromium-browser")
    if not chromium:
        raise DomainError(
            "browser_dependency_missing", "Chromium is missing from the reviewed worker"
        )
    with tempfile.TemporaryDirectory(prefix="ga-browser-") as directory:
        source = Path(directory) / "page.html"
        safe_content, removed = static_html(content)
        source.write_text(safe_content, encoding="utf-8")
        configuration, cache = Path(directory) / "config", Path(directory) / "cache"
        configuration.mkdir()
        cache.mkdir()
        configured_home = os.environ.get("HOME")
        if configured_home and Path(configured_home).is_relative_to("/tmp"):
            Path(configured_home).mkdir(parents=True, exist_ok=True, mode=0o700)
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(
                executable_path=chromium,
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-background-networking"],
                env={
                    **os.environ,
                    "XDG_CONFIG_HOME": str(configuration),
                    "XDG_CACHE_HOME": str(cache),
                },
            )
            try:
                context = await browser.new_context(
                    viewport={"width": 1280, "height": 900},
                    accept_downloads=False,
                    service_workers="block",
                    permissions=[],
                    java_script_enabled=False,
                )

                async def guard(route):
                    if (
                        route.request.url == source.as_uri()
                        and route.request.is_navigation_request()
                    ):
                        await route.continue_()
                    else:
                        await route.abort()

                await context.route("**/*", guard)
                page = await context.new_page()
                await page.goto(source.as_uri(), wait_until="load", timeout=30000)
                await page.wait_for_timeout(150)
                if page.url != source.as_uri():
                    raise DomainError(
                        "browser_navigation_blocked", "Offline page attempted another navigation"
                    )
                title = await page.title()
                text = await page.locator("body").inner_text(timeout=5000)
                lower = text.lower()
                blocked = any(
                    marker in lower
                    for marker in (
                        "verify you are human",
                        "captcha challenge",
                        "complete the captcha",
                        "请输入验证码",
                        "人机验证",
                        "access denied",
                    )
                )
                await page.screenshot(path=str(output), full_page=False, timeout=10000)
                return {
                    "title": title[:1000],
                    "text": text[:100000],
                    "truncated": len(text) > 100000,
                    "access_blocked": blocked,
                    "external_resources_blocked": True,
                    "javascript_enabled": False,
                    "active_content_removed": removed,
                }
            finally:
                await browser.close()


def browser_snapshot(content: bytes, params: dict[str, Any], output: Path) -> dict[str, Any]:
    if not content or len(content) > 4 * 1024 * 1024:
        raise DomainError("browser_page_limit", "Browser page is empty or too large")
    try:
        return asyncio.run(_snapshot(content, output))
    except DomainError:
        raise
    except Exception:
        raise DomainError(
            "browser_render_failed", "Isolated browser could not render the page"
        ) from None
