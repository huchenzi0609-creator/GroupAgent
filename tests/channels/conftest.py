from __future__ import annotations

import hashlib
import json

import httpx
import pytest

from groupagent.channels import AgentWechatAdapter, AgentWechatConfig

ACCOUNT = "synthetic-account"
ROOM = "synthetic-room@chatroom"
GROUP = "synthetic-group"
TOKEN = "synthetic-token-not-a-credential"


def message(identifier="local-1", *, kind=1, **changes):
    return {
        "id": identifier,
        "accountId": ACCOUNT,
        "threadId": ROOM,
        "serverId": "18446744073709551000",
        "senderId": "synthetic-member",
        "type": kind,
        "direction": "incoming",
        "body": "A synthetic classroom message",
        "createdAt": "2026-11-13T08:00:00Z",
        "version": "profile-fingerprint-1",
        "mentions": {"state": "unknown", "userIds": [], "self": None, "all": None, "source": None},
        "media": [],
        **changes,
    }


def asset(message_id="local-1", *, kind="image", content=b"synthetic-image", **changes):
    return {
        "id": "asset-1",
        "accountId": ACCOUNT,
        "messageId": message_id,
        "kind": kind,
        "variant": "normal" if kind == "voice" else "original",
        "actualVariant": "original",
        "state": "available",
        "contentType": "image/png"
        if kind == "image"
        else "audio/wav"
        if kind == "voice"
        else "application/pdf",
        "byteLength": len(content),
        "sha256": hashlib.sha256(content).hexdigest(),
        "filename": "image.png"
        if kind == "image"
        else "voice.wav"
        if kind == "voice"
        else "report.pdf",
        "reason": None,
        "jobId": None,
        "retryable": False,
        "expiresAt": None,
        "contentUrl": "https://untrusted.invalid/never-fetch",
        "officialWriteback": "complete",
        **changes,
    }


class FakeDaemon:
    """HTTP response fixtures only: no process, account, WeChat transport or live send."""

    def __init__(self):
        self.requests = []
        self.messages = {"local-1": message()}
        self.assets = {}
        self.content = b"synthetic-image"
        self.events = []
        self.next_cursor = "0"
        self.version = "2.2.1"
        self.server_id = "synthetic-daemon-storage"
        self.sent = {}
        self.send_bodies = {}
        self.send_calls = 0
        self.upload_calls = 0
        self.upload_name = "report.md"
        self.upload_content = b"# Synthetic report"
        self.failure = None
        self.roster = []
        self.member_state = "present"

    def receipt(self, *, state="accepted", **changes):
        return {
            "id": "send-1",
            "accountId": ACCOUNT,
            "destination": ROOM,
            "state": state,
            "clientId": "client-1",
            "serverId": "18446744073709551001" if state == "accepted" else None,
            "errorCode": None,
            "createdAt": "2026-11-13T08:01:00Z",
            **changes,
        }

    def __call__(self, request):
        self.requests.append(request)
        path = request.url.path
        prefix = f"/v1/accounts/{ACCOUNT}/"
        assert request.headers["Authorization"] == "Bearer " + TOKEN
        if self.failure:
            result = self.failure(request)
            if result is not None:
                return result
        if path == "/v1/status":
            return httpx.Response(
                200,
                json={
                    "serverId": self.server_id,
                    "dataDir": "/do-not-expose/daemon-data",
                    "pid": 123,
                    "ownershipGeneration": "process-1",
                    "state": "ready",
                    "version": {
                        "product": "agent-wechat",
                        "backend": "native",
                        "version": self.version,
                        "apiVersion": 1,
                        "capabilities": ["text-send", "events", "scoped-tokens"],
                    },
                },
            )
        assert path.startswith(prefix)
        route = path[len(prefix) :]
        if route == "capabilities":
            return httpx.Response(
                200,
                json={
                    "nativeRuntime": True,
                    "mediaUploadMaxBytes": 536870912,
                    "mediaSendMaxBytes": {
                        "file": 536870912,
                        "image": 26214383,
                        "voice": 26214383,
                        "video": 536870912,
                    },
                    "mediaKinds": ["image", "file", "voice", "video"],
                    "quotedTextSend": True,
                },
            )
        if route == "events":
            return httpx.Response(200, json={"items": self.events, "nextCursor": self.next_cursor})
        if route.startswith("consumers/") and route.endswith("/ack"):
            return httpx.Response(200, json={"state": "recorded"})
        if route.startswith("messages/"):
            rest = route[len("messages/") :]
            if rest.endswith("/media"):
                return httpx.Response(200, json={"items": self.assets.get(rest[:-6], [])})
            if rest.endswith("/media-transfers"):
                body = json.loads(request.content)
                return httpx.Response(
                    202,
                    json={
                        "id": "transfer-1",
                        "accountId": ACCOUNT,
                        "kind": "download",
                        "state": "queued",
                        "assetId": body["assetId"],
                    },
                )
            if rest in self.messages:
                return httpx.Response(200, json=self.messages[rest])
        if route == "media/asset-1/content":
            return httpx.Response(200, content=self.content, headers={"Content-Type": "image/png"})
        if route == f"groups/{ROOM}/members":
            return httpx.Response(
                200,
                json={
                    "items": self.roster,
                    "nextCursor": None,
                    "sourceGeneration": "fingerprint-1",
                    "coverage": {"partial": True},
                },
            )
        if route.startswith(f"groups/{ROOM}/members/"):
            member_id = route.rsplit("/", 1)[1]
            member = next((row for row in self.roster if row["id"] == member_id), None)
            return httpx.Response(
                200,
                json={
                    "groupId": ROOM,
                    "username": member_id,
                    "state": self.member_state,
                    "member": member,
                    "coverage": "cached",
                },
            )
        if route == "sends/by-idempotency-key":
            found = self.sent.get(request.url.params["key"])
            return (
                httpx.Response(200, json=found)
                if found
                else httpx.Response(404, json={"error": {"code": "not_found"}})
            )
        if route == "uploads":
            self.upload_calls += 1
            return httpx.Response(
                201,
                json={
                    "id": "source-1",
                    "name": self.upload_name,
                    "mediaType": "text/markdown",
                    "byteLength": len(self.upload_content),
                    "sha256": hashlib.sha256(self.upload_content).hexdigest(),
                    "createdAt": "2026-11-13T08:01:00Z",
                },
            )
        if route == "sends":
            key = request.headers["Idempotency-Key"]
            body = json.loads(request.content)
            self.send_calls += 1
            if key in self.send_bodies and self.send_bodies[key] != body:
                return httpx.Response(409, json={"error": {"code": "idempotency_conflict"}})
            self.send_bodies[key] = body
            if body["kind"] in {"file", "image", "voice"}:
                result = {
                    "id": "job-1",
                    "accountId": ACCOUNT,
                    "threadId": ROOM,
                    "kind": "send",
                    "state": "queued",
                    "serverMessageId": None,
                    "reason": None,
                }
            else:
                result = self.receipt(state="queued")
            self.sent.setdefault(key, result)
            return httpx.Response(202, json=self.sent[key])
        return httpx.Response(404, json={"error": {"code": "not_found"}})


@pytest.fixture
def config():
    return AgentWechatConfig(
        base_url="https://synthetic.invalid",
        account_id=ACCOUNT,
        external_group_id=ROOM,
        internal_group_id=GROUP,
        source_epoch="epoch-1",
    )


@pytest.fixture
def daemon():
    return FakeDaemon()


@pytest.fixture
async def adapter(config, daemon):
    async with httpx.AsyncClient(transport=httpx.MockTransport(daemon)) as client:
        yield AgentWechatAdapter(config, TOKEN, client=client)
