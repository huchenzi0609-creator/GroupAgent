from __future__ import annotations

import httpx
import pytest

from groupagent.domain import DomainError
from groupagent.research.browser import PublicPageBroker


async def resolver(host):
    return ("93.184.216.34",)


async def test_broker_pins_public_ip_and_does_not_forward_client_credentials():
    observed = []

    def response(request):
        observed.append(request)
        return httpx.Response(
            200,
            content=b"<html><title>Original source</title><body>Verified source text.</body></html>",
            headers={"Content-Type": "text/html", "Set-Cookie": "private=value"},
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(response),
        headers={"Authorization": "Bearer synthetic-secret"},
        cookies={"session": "synthetic-cookie"},
    ) as client:
        broker = PublicPageBroker(frozenset({"example.com"}), resolver=resolver, client=client)
        first = await broker.fetch("https://example.com/source")
        await broker.fetch("https://example.com/second")
    assert first.final_url == "https://example.com/source"
    assert all(request.url.host == "93.184.216.34" for request in observed)
    assert all(request.headers["Host"] == "example.com" for request in observed)
    assert all(
        "Authorization" not in request.headers and "Cookie" not in request.headers
        for request in observed
    )
    assert observed[0].extensions["sni_hostname"] == "example.com"


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1/",
        "https://metadata.google.internal/",
        "file:///etc/passwd",
        "https://example.com/video.mp4",
    ],
)
async def test_nonpublic_or_video_targets_never_fetch(url):
    calls = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: calls.append(request))
    ) as client:
        broker = PublicPageBroker(frozenset({"example.com"}), resolver=resolver, client=client)
        with pytest.raises(DomainError):
            await broker.fetch(url)
    assert not calls


async def test_private_dns_and_redirect_target_are_refused():
    async def private(host):
        return ("127.0.0.1",)

    calls = []

    def redirect(request):
        calls.append(request)
        return httpx.Response(302, headers={"Location": "http://127.0.0.1/private"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(redirect)) as client:
        with pytest.raises(DomainError):
            await PublicPageBroker(
                frozenset({"example.com"}), resolver=private, client=client
            ).fetch("https://example.com/")
        assert not calls
        with pytest.raises(DomainError):
            await PublicPageBroker(
                frozenset({"example.com"}), resolver=resolver, client=client
            ).fetch("https://example.com/")
    assert len(calls) == 1


@pytest.mark.parametrize(
    "body,status,mime,code",
    [
        (b"Verify you are human", 200, "text/html", "browser_captcha"),
        (b"Forbidden", 403, "text/html", "browser_access_blocked"),
        (b"video", 200, "video/mp4", "browser_non_html"),
    ],
)
async def test_access_challenges_and_non_html_are_explicit(body, status, mime, code):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(status, content=body, headers={"Content-Type": mime})
        )
    ) as client:
        broker = PublicPageBroker(frozenset({"example.com"}), resolver=resolver, client=client)
        with pytest.raises(DomainError) as error:
            await broker.fetch("https://example.com/")
        assert error.value.code == code
