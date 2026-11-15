"""Actual pinned Harness SDK process inside a networkless container.

The native runtime talks only to a loopback model/MCP relay. The relay has no
provider key: host-side EnginePort decides and budgets every model/tool request.
Public SDK interfaces verified against the source hashes in harness-manifest.json.
"""

from __future__ import annotations

import base64
import io
import json
import os
import queue
import tarfile
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path, PurePosixPath
from typing import Any

from groupagent.sandbox.supervisor import resource_usage

from .harness_wire import chat_to_message, message_events, messages_to_chat

MAX_CHECKPOINT_BYTES = 8 * 1024 * 1024
_output_lock = threading.Lock()


def emit(value: dict) -> None:
    with _output_lock:
        print(json.dumps(value, ensure_ascii=True, separators=(",", ":")), flush=True)


def restore_sessions(home: Path, encoded: str) -> None:
    data = base64.b64decode(encoded, validate=True)
    if len(data) > MAX_CHECKPOINT_BYTES:
        raise ValueError("checkpoint too large")
    total = 0
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
        entries = archive.getmembers()
        if len(entries) > 512:
            raise ValueError("too many checkpoint entries")
        for entry in entries:
            path = PurePosixPath(entry.name)
            if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
                raise ValueError("unsafe checkpoint path")
            if not entry.isfile() or entry.size < 0:
                raise ValueError("checkpoint must contain regular files only")
            total += entry.size
            if total > MAX_CHECKPOINT_BYTES:
                raise ValueError("checkpoint expands beyond its limit")
            source = archive.extractfile(entry)
            assert source is not None
            destination = home / "sessions" / str(path)
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(source.read())


def capture_sessions(home: Path) -> str:
    stream, total, count = io.BytesIO(), 0, 0
    with tarfile.open(fileobj=stream, mode="w:gz") as archive:
        sessions = home / "sessions"
        if sessions.exists():
            for path in sorted(sessions.rglob("*")):
                if path.is_symlink():
                    raise ValueError("checkpoint symlink")
                if path.is_file():
                    total += path.stat().st_size
                    count += 1
                    if total > MAX_CHECKPOINT_BYTES or count > 512:
                        raise ValueError("checkpoint exceeds limit")
                    archive.add(path, arcname=str(path.relative_to(sessions)), recursive=False)
    return base64.b64encode(stream.getvalue()).decode("ascii")


def profile_patch(port: int) -> list[dict]:
    patch: list[dict[str, Any]] = [{"id": "session-log-deepseek", "config": {"enabled": False}}]
    for name in (
        "plugin-package-inventory-deepseek",
        "tool-fs",
        "tool-working-directory",
        "persistent-bash",
        "persistent-pwsh",
        "mcp-resources",
        "llm-retry",
        "compaction-basic",
        "image-offload",
        "session-title",
    ):
        patch.append({"id": name, "disabled": True})
    patch.append(
        {
            "insert": [
                {
                    "id": "groupagent-scoped-gateway",
                    "name": "@deepseek-ai/dsh-mcp-client",
                    "config": {
                        "serverName": "groupagent",
                        "transport": "streamable-http",
                        "url": f"http://127.0.0.1:{port}/mcp",
                        "startupTimeoutMs": 10000,
                        "toolCallTimeoutMs": 600000,
                        "failOnStartupError": True,
                        "reconnect": {"enabled": False},
                    },
                }
            ]
        }
    )
    return patch


class Relay:
    def __init__(self, initial: dict):
        self.initial = initial
        self.pending: dict[str, queue.Queue] = {}
        self.lock = threading.Lock()
        self.harness: Any = None
        self.ready = threading.Event()
        self.closed = threading.Event()

    def request(self, method: str, payload: dict) -> dict:
        request_id = uuid.uuid4().hex
        mailbox: queue.Queue = queue.Queue(maxsize=1)
        with self.lock:
            self.pending[request_id] = mailbox
        emit({"type": "request", "id": request_id, "method": method, "payload": payload})
        try:
            response = mailbox.get(timeout=650)
            if "error" in response:
                raise ValueError("Host relay rejected request")
            return response["result"]
        finally:
            with self.lock:
                self.pending.pop(request_id, None)

    def read_commands(self) -> None:
        import sys

        for line in sys.stdin:
            if len(line) > 16_000_000:
                break
            try:
                message = json.loads(line)
                if "response_to" in message:
                    with self.lock:
                        mailbox = self.pending.get(message["response_to"])
                    if mailbox is not None:
                        mailbox.put_nowait(message)
                elif message.get("command") == "append":
                    self.ready.wait(timeout=30)
                    if self.harness is not None and not self.closed.is_set():
                        self.harness.client.session_prompt(
                            self.initial["session_id"],
                            [{"type": "text", "text": message["text"]}],
                        )
                        emit(
                            {
                                "type": "input",
                                "revision": message["revision"],
                                "input_id": message["input_id"],
                            }
                        )
            except Exception:
                emit({"type": "protocol_error"})
                break
        # A dead host controller cannot leave the SDK resident for its long
        # inference timeout. Fail every pending relay request, then close SDK.
        self.closed.set()
        with self.lock:
            for mailbox in self.pending.values():
                try:
                    mailbox.put_nowait({"error": "controller_disconnected"})
                except queue.Full:
                    pass
        if self.harness is not None:
            self.harness.close()


def handler(relay: Relay):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def send_json(self, status: int, value: dict) -> None:
            data = json.dumps(value, ensure_ascii=True).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            self.send_error(405)

        def do_DELETE(self):
            self.send_response(200)
            self.end_headers()

        def do_POST(self):
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= 2_000_000:
                    self.send_error(413)
                    return
                body = json.loads(self.rfile.read(length))
                if self.path == "/mcp":
                    method = body.get("method")
                    if "id" not in body:
                        self.send_response(202)
                        self.end_headers()
                        return
                    if method == "initialize":
                        result = {
                            "protocolVersion": body["params"]["protocolVersion"],
                            "capabilities": {"tools": {}},
                            "serverInfo": {"name": "groupagent-scoped-gateway", "version": "1"},
                        }
                    elif method == "tools/list":
                        result = {
                            "tools": [
                                {
                                    "name": tool["function"]["name"],
                                    "description": tool["function"]["description"],
                                    "inputSchema": tool["function"]["parameters"],
                                }
                                for tool in relay.initial["tools"]
                            ]
                        }
                    elif method == "tools/call":
                        result = relay.request("tool", body["params"])
                    elif method == "ping":
                        result = {}
                    else:
                        self.send_json(
                            200,
                            {
                                "jsonrpc": "2.0",
                                "id": body["id"],
                                "error": {"code": -32601, "message": "Method unavailable"},
                            },
                        )
                        return
                    self.send_json(200, {"jsonrpc": "2.0", "id": body["id"], "result": result})
                elif self.path.endswith("/messages"):
                    result = chat_to_message(relay.request("model", messages_to_chat(body)))
                    if not body.get("stream"):
                        self.send_json(200, result)
                        return
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream")
                    self.send_header("Cache-Control", "no-cache")
                    self.end_headers()
                    for event in message_events(result):
                        self.wfile.write(
                            (
                                "event: " + event["type"] + "\ndata: " + json.dumps(event) + "\n\n"
                            ).encode()
                        )
                    self.wfile.flush()
                elif self.path.endswith("/chat/completions"):
                    result = relay.request("model", body)
                    if not body.get("stream"):
                        self.send_json(200, result)
                        return
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream")
                    self.send_header("Cache-Control", "no-cache")
                    self.end_headers()
                    choice = result["choices"][0]
                    message = choice["message"]
                    delta = {"role": "assistant", "content": message.get("content") or ""}
                    if message.get("tool_calls"):
                        delta["tool_calls"] = [
                            {"index": index, **call}
                            for index, call in enumerate(message["tool_calls"])
                        ]
                    chunk = {
                        "id": result["id"],
                        "object": "chat.completion.chunk",
                        "created": 0,
                        "model": result["model"],
                        "choices": [{"index": 0, "delta": delta, "finish_reason": None}],
                    }
                    self.wfile.write(("data: " + json.dumps(chunk) + "\n\n").encode())
                    chunk["choices"] = [
                        {"index": 0, "delta": {}, "finish_reason": choice["finish_reason"]}
                    ]
                    chunk["usage"] = result["usage"]
                    self.wfile.write(
                        ("data: " + json.dumps(chunk) + "\n\ndata: [DONE]\n\n").encode()
                    )
                    self.wfile.flush()
                else:
                    self.send_error(404)
            except Exception:
                self.send_json(
                    500,
                    {
                        "error": {
                            "message": "Scoped host relay refused operation",
                            "type": "server_error",
                        }
                    },
                )

    return Handler


def main() -> int:
    import sys

    from deepseek_harness import DeepSeekHarness

    initial = json.loads(sys.stdin.readline(16_000_001))
    relay = Relay(initial)
    # Native addon-system also uses the Harness home cache. Keep it on the
    # SDK-only executable tmpfs; user work remains on the noexec /work mount.
    home, workspace = Path("/tmp/harness-home"), Path("/work/workspace")
    home.mkdir(mode=0o700)
    workspace.mkdir(mode=0o700)
    if initial.get("sessions"):
        restore_sessions(home, initial["sessions"])
    for root in (home, workspace):
        for directory, _, filenames in os.walk(root):
            os.chown(directory, 65532, 65532)
            for filename in filenames:
                os.chown(Path(directory) / filename, 65532, 65532)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler(relay))
    server.daemon_threads = True
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    patch = Path("/tmp/groupagent-profile.yml")
    patch.write_text(json.dumps(profile_patch(port)))
    threading.Thread(target=relay.read_commands, daemon=True).start()
    harness = DeepSeekHarness(
        provider="deepseek-official",
        model="deepseek-flash",
        max_tokens=initial["max_tokens"],
        dsh_home=str(home),
        cwd=str(workspace),
        runtime_cwd=str(workspace),
        dsh_bin="/opt/groupagent-dropuid-dsh",
        profile="sdk-minimal",
        patches=(str(patch),),
        base_url=f"http://127.0.0.1:{port}/v1",
        api_key="groupagent-scoped-relay-placeholder",
        env={
            "HOME": str(home),
            "DSH_CONTEXT_WINDOW": "1000000",
            "DSH_SYSTEM_PROMPT": initial["system_prompt"],
        },
        initialize_timeout_seconds=30,
        request_timeout_seconds=30,
        shutdown_timeout_seconds=3,
    )
    relay.harness = harness
    try:
        harness.start()
        relay.ready.set()
        session = (
            harness.resume_session(initial["session_id"])
            if initial.get("sessions")
            else harness.start_session(initial["session_id"])
        )
        result = session.run(initial["prompt"])
        harness.close()
        completion = {
            "type": "finished",
            "finish_reason": result.finish_reason,
            "text": result.final_response,
            "sessions": capture_sessions(home),
            "resource_usage": resource_usage(),
        }
        if initial.get("synthetic_diagnostics"):
            completion["diagnostics"] = [
                event for event in result.events if event.get("type") in {"turn/end", "error"}
            ][-3:]
        emit(completion)
        return 0
    except Exception as exc:
        try:
            harness.close()
        except Exception:
            pass
        failure = {"type": "failed", "error_type": type(exc).__name__}
        if initial.get("synthetic_diagnostics"):
            failure["diagnostic"] = str(exc)[-8000:]
        emit(failure)
        return 2
    finally:
        relay.closed.set()
        server.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
