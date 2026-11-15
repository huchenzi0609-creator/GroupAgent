#!/usr/bin/env python3
"""Root-owned, stdlib-only Docker API broker for one pinned Pot worker group.

This is deliberately not a general Docker socket proxy. It accepts the narrow
CLI protocol used by DockerSandbox, validates every create field, checks daemon
ownership before every container operation, and never forwards arbitrary paths.
"""

from __future__ import annotations

import argparse
import http.client
import importlib.util
import json
import os
import re
import socket
import socketserver
import stat
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qsl, unquote, urlencode, urlsplit

# The root service uses python -I. Load only this root-owned sibling file;
# neither the controller's cwd nor user package paths participate in imports.
_CAPACITY_SPEC = importlib.util.spec_from_file_location(
    "_pot_capacity", Path(__file__).resolve().with_name("pot_capacity.py")
)
if _CAPACITY_SPEC is None or _CAPACITY_SPEC.loader is None:
    raise RuntimeError("Bundled CPU capacity policy is unavailable")
capacity = importlib.util.module_from_spec(_CAPACITY_SPEC)
sys.modules[_CAPACITY_SPEC.name] = capacity
_CAPACITY_SPEC.loader.exec_module(capacity)

BODY_LIMIT = 256 * 1024
RESPONSE_LIMIT = 2 * 1024 * 1024
MEMORY = 512 * 1024 * 1024
CONTAINER_NAME = re.compile(r"groupagent-[a-f0-9]{32}")
CONTAINER_ID = re.compile(r"[a-f0-9]{64}")
GROUP_HASH = re.compile(r"[a-f0-9]{64}")
TMPFS = {
    "/work": "rw,noexec,nosuid,nodev,size=128m,mode=0770,uid=65532,gid=65532",
    "/tmp": "rw,noexec,nosuid,nodev,size=64m,mode=1777",
}
CAPS = {"SETUID", "SETGID", "KILL", "CHOWN", "DAC_OVERRIDE"}


class Denied(Exception):
    def __init__(self, status: int = 403, message: str = "Pot worker policy denied this request"):
        super().__init__(message)
        self.status, self.message = status, message


def strict_json(raw: bytes):
    def pairs(values):
        result = {}
        for key, value in values:
            if key in result:
                raise ValueError("Duplicate JSON key")
            result[key] = value
        return result

    try:
        return json.loads(
            raw,
            object_pairs_hook=pairs,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError("Non-finite JSON")),
        )
    except (ValueError, UnicodeError, RecursionError):
        raise Denied(400, "Invalid JSON") from None


def encoded(value) -> bytes:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":")).encode()


@dataclass(frozen=True)
class Policy:
    image: str
    group_hash: str

    def __post_init__(self):
        if not re.fullmatch(r"sha256:[a-f0-9]{64}", self.image):
            raise ValueError("Use an immutable local worker image ID")
        if not GROUP_HASH.fullmatch(self.group_hash):
            raise ValueError("Use the trusted group SHA-256 label")


def validate_create(policy: Policy, value) -> None:
    """Unknown keys fail closed, including apparently harmless false defaults."""
    if not isinstance(value, dict):
        raise Denied()
    required = {
        "Image": policy.image,
        "User": "0:0",
        "WorkingDir": "/work",
        "OpenStdin": True,
        "AttachStdin": True,
        "AttachStdout": True,
        "AttachStderr": True,
    }
    optional = {
        "Hostname": ("",),
        "Domainname": ("",),
        "Tty": (False,),
        "StdinOnce": (True, False),
        "NetworkDisabled": (False,),
        "ArgsEscaped": (False,),
        "StopSignal": ("", None),
        "StopTimeout": (None,),
        "Cmd": (None,),
        "Entrypoint": (None,),
        "Env": (None, []),
        "Volumes": (None, {}),
        "ExposedPorts": (None, {}),
        "Healthcheck": (None,),
        "Shell": (None,),
        "MacAddress": ("",),
        "OnBuild": (None,),
    }
    if set(value) - set(required) - set(optional) - {"Labels", "HostConfig", "NetworkingConfig"}:
        raise Denied()
    for key, expected in required.items():
        if type(value.get(key)) is not type(expected) or value[key] != expected:
            raise Denied()
    for key, choices in optional.items():
        if key in value and not any(
            type(value[key]) is type(item) and value[key] == item for item in choices
        ):
            raise Denied()
    labels = value.get("Labels")
    if (
        not isinstance(labels, dict)
        or set(labels) != {"org.groupagent.managed", "org.groupagent.group", "org.groupagent.scope"}
        or labels["org.groupagent.managed"] != "1"
        or labels["org.groupagent.group"] != policy.group_hash
        or not isinstance(labels["org.groupagent.scope"], str)
        or not GROUP_HASH.fullmatch(labels["org.groupagent.scope"])
    ):
        raise Denied()
    host = value.get("HostConfig")
    if not isinstance(host, dict):
        raise Denied()
    exact = {
        "NetworkMode": "none",
        "ReadonlyRootfs": True,
        "Init": True,
        "Memory": MEMORY,
        "MemorySwap": MEMORY,
        "NanoCpus": 750_000_000,
        "CpuShares": 128,
        "OomScoreAdj": 500,
        "PidsLimit": 192,
        "Tmpfs": TMPFS,
        "SecurityOpt": ["no-new-privileges:true"],
        "CapDrop": ["ALL"],
        "Ulimits": [{"Name": "nofile", "Hard": 256, "Soft": 256}],
    }
    empty_lists = {
        "Binds",
        "Mounts",
        "Devices",
        "DeviceRequests",
        "DeviceCgroupRules",
        "VolumesFrom",
        "Links",
        "ExtraHosts",
        "Dns",
        "DnsOptions",
        "DnsSearch",
        "GroupAdd",
        "BlkioWeightDevice",
        "BlkioDeviceReadBps",
        "BlkioDeviceWriteBps",
        "BlkioDeviceReadIOps",
        "BlkioDeviceWriteIOps",
    }
    empty_dicts = {"PortBindings", "Sysctls", "StorageOpt", "Annotations"}
    zeroes = {
        "MemoryReservation",
        "CpuPeriod",
        "CpuQuota",
        "CpuRealtimePeriod",
        "CpuRealtimeRuntime",
        "KernelMemory",
        "KernelMemoryTCP",
        "CpuCount",
        "CpuPercent",
        "IOMaximumIOps",
        "IOMaximumBandwidth",
        "BlkioWeight",
    }
    empty_strings = {
        "CgroupParent",
        "ContainerIDFile",
        "VolumeDriver",
        "Runtime",
        "PidMode",
        "UTSMode",
        "UsernsMode",
        "CpusetCpus",
        "CpusetMems",
        "Cgroup",
    }
    extra = {
        "Privileged",
        "AutoRemove",
        "PublishAllPorts",
        "OomKillDisable",
        "MemorySwappiness",
        "CgroupnsMode",
        "IpcMode",
        "Isolation",
        "ShmSize",
        "ConsoleSize",
        "RestartPolicy",
        "LogConfig",
        "MaskedPaths",
        "ReadonlyPaths",
        "CapAdd",
    }
    if set(host) - set(exact) - empty_lists - empty_dicts - zeroes - empty_strings - extra:
        raise Denied()
    for key, expected in exact.items():
        if type(host.get(key)) is not type(expected) or host[key] != expected:
            raise Denied()
    for key in empty_lists:
        if host.get(key) is not None and host[key] != []:
            raise Denied()
    for key in empty_dicts:
        if host.get(key) is not None and host[key] != {}:
            raise Denied()
    for key in zeroes:
        if key in host and (type(host[key]) is not int or host[key] != 0):
            raise Denied()
    for key in empty_strings:
        if key in host and host[key] != "":
            raise Denied()
    for key in ("Privileged", "AutoRemove", "PublishAllPorts"):
        if key in host and (type(host[key]) is not bool or host[key]):
            raise Denied()
    swappiness = host.get("MemorySwappiness")
    if host.get("OomKillDisable") not in (None, False) or not (
        swappiness is None or (type(swappiness) is int and swappiness == -1)
    ):
        raise Denied()
    for key in ("MaskedPaths", "ReadonlyPaths"):
        if host.get(key) is not None:
            raise Denied()  # [] would disable Docker's secure default masks.
    if host.get("CgroupnsMode", "") not in ("", "private") or host.get("IpcMode", "") not in (
        "",
        "private",
    ):
        raise Denied()
    if host.get("Isolation", "") not in ("", "default"):
        raise Denied()
    if type(host.get("ShmSize", 0)) is not int or host.get("ShmSize", 0) not in (
        0,
        64 * 1024 * 1024,
    ):
        raise Denied()
    if host.get("ConsoleSize", [0, 0]) != [0, 0]:
        raise Denied()
    restart = host.get("RestartPolicy", {})
    if (
        not isinstance(restart, dict)
        or set(restart) - {"Name", "MaximumRetryCount"}
        or restart.get("Name", "") not in ("", "no")
        or type(restart.get("MaximumRetryCount", 0)) is not int
        or restart.get("MaximumRetryCount", 0) != 0
    ):
        raise Denied()
    log = host.get("LogConfig", {})
    if (
        not isinstance(log, dict)
        or set(log) - {"Type", "Config"}
        or log.get("Type", "") not in ("", "none")
        or log.get("Config", {}) not in (None, {})
    ):
        raise Denied()
    caps = host.get("CapAdd")
    if (
        not isinstance(caps, list)
        or len(caps) != len(CAPS)
        or any(not isinstance(cap, str) for cap in caps)
        or {cap.removeprefix("CAP_") for cap in caps} != CAPS
    ):
        raise Denied()
    networking = value.get("NetworkingConfig")
    if networking not in (None, {}):
        if not isinstance(networking, dict) or set(networking) != {"EndpointsConfig"}:
            raise Denied()
        endpoints = networking["EndpointsConfig"]
        if not isinstance(endpoints, dict) or set(endpoints) not in (set(), {"none"}):
            raise Denied()
        endpoint = endpoints.get("none")
        if endpoint not in (None, {}):
            if not isinstance(endpoint, dict) or set(endpoint) - {
                "IPAMConfig",
                "Links",
                "Aliases",
                "MacAddress",
                "DriverOpts",
                "GwPriority",
            }:
                raise Denied()
            if any(value not in (None, "", [], {}, 0) for value in endpoint.values()):
                raise Denied()


@dataclass
class Response:
    status: int
    body: bytes = b""
    headers: dict[str, str] | None = None


@dataclass
class Attach:
    target: str


@dataclass
class Wait:
    target: str


class UnixConnection(http.client.HTTPConnection):
    def __init__(self, path: str, timeout: float = 15):
        super().__init__("docker", timeout=timeout)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.path)


class Backend:
    def __init__(self, path: str):
        self.path = path

    def request(
        self, method: str, target: str, body: bytes = b"", *, wait=False, timeout=None
    ) -> Response:
        connection = UnixConnection(self.path, timeout=timeout or (660 if wait else 15))
        try:
            connection.request(
                method, target, body=body, headers={"Content-Type": "application/json"}
            )
            response = connection.getresponse()
            data = response.read(RESPONSE_LIMIT + 1)
            if len(data) > RESPONSE_LIMIT:
                raise Denied(502, "Docker response exceeds its bound")
            allowed = {
                "content-type",
                "api-version",
                "docker-experimental",
                "ostype",
                "builder-version",
            }
            headers = {key: value for key, value in response.getheaders() if key.lower() in allowed}
            return Response(response.status, data, headers)
        finally:
            connection.close()

    def attach(self, target: str) -> socket.socket:
        upstream = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            upstream.settimeout(660)
            upstream.connect(self.path)
            upstream.sendall(
                (
                    "POST "
                    + target
                    + " HTTP/1.1\r\nHost: docker\r\nConnection: Upgrade\r\nUpgrade: tcp\r\nContent-Length: 0\r\n\r\n"
                ).encode("ascii")
            )
            return upstream
        except BaseException:
            upstream.close()
            raise

    def open_wait(self, target: str):
        connection = UnixConnection(self.path, timeout=660)
        try:
            connection.request("POST", target, body=b"")
            return connection, connection.getresponse()
        except BaseException:
            connection.close()
            raise


class Broker:
    def __init__(self, policy: Policy, backend: Backend):
        self.policy, self.backend = policy, backend
        self.creation_lock = threading.Lock()
        self.cpu = capacity.CapacityControl()
        self.host_sampler = capacity.HostSampler()
        self._capacity_stop, self._capacity_wake = threading.Event(), threading.Event()
        self._capacity_thread: threading.Thread | None = None
        self._capacity_lifecycle = threading.Lock()
        self._capacity_state = threading.RLock()
        self._worker_counters: dict[str, tuple[int, float]] = {}
        self._applied_cpu: dict[str, float] = {}
        self._last_capacity_error = 0

    def owned(self, prefix: str, reference: str, *, timeout=None) -> str:
        if not (CONTAINER_NAME.fullmatch(reference) or re.fullmatch(r"[a-f0-9]{12,64}", reference)):
            raise Denied()
        options = {"timeout": timeout} if timeout is not None else {}
        response = self.backend.request(
            "GET", prefix + "/containers/" + reference + "/json", **options
        )
        if response.status != 200:
            raise Denied(404, "Owned worker not found")
        value = strict_json(response.body)
        try:
            labels = value["Config"]["Labels"]
            identifier = value["Id"]
            valid = (
                value["Image"] == self.policy.image
                and labels["org.groupagent.managed"] == "1"
                and labels["org.groupagent.group"] == self.policy.group_hash
                and isinstance(identifier, str)
                and CONTAINER_ID.fullmatch(identifier)
            )
        except (KeyError, TypeError):
            valid = False
        if not valid:
            raise Denied()
        return identifier

    def inventory(self, prefix: str, filters: dict | None = None, *, timeout=None) -> list:
        filters = dict(filters or {})
        labels = list(filters.get("label", []))
        for value in ("org.groupagent.managed=1", "org.groupagent.group=" + self.policy.group_hash):
            if value not in labels:
                labels.append(value)
        filters["label"] = labels
        target = (
            prefix
            + "/containers/json?"
            + urlencode({"all": "1", "filters": encoded(filters).decode()})
        )
        options = {"timeout": timeout} if timeout is not None else {}
        response = self.backend.request("GET", target, **options)
        if response.status != 200:
            raise Denied(502, "Owned worker inventory is unavailable")
        values = strict_json(response.body)
        if not isinstance(values, list) or len(values) > 1000:
            raise Denied(502, "Owned worker inventory is invalid")
        return [
            value
            for value in values
            if isinstance(value, dict)
            and isinstance(value.get("Labels"), dict)
            and value["Labels"].get("org.groupagent.managed") == "1"
            and value["Labels"].get("org.groupagent.group") == self.policy.group_hash
        ]

    @staticmethod
    def capacity_log(code: int, **values):
        print(encoded({"code": code, **values}).decode(), flush=True)

    def capacity_tick(self) -> None:
        """Internal-only Docker stats/update; never called with a client target."""
        now = self.cpu.clock()
        host = self.host_sampler.sample(now)
        running = []
        known_inventory = False
        worker_busy = None
        try:
            items = self.inventory("", timeout=1)
            running = [
                item["Id"]
                for item in items
                if item.get("ImageID") == self.policy.image
                and item.get("State") == "running"
                and isinstance(item.get("Id"), str)
                and CONTAINER_ID.fullmatch(item["Id"])
            ]
            known_inventory = True
            with self._capacity_state:
                self._worker_counters = {
                    key: value for key, value in self._worker_counters.items() if key in running
                }
                self._applied_cpu = {
                    key: value for key, value in self._applied_cpu.items() if key in running
                }
            if not running:
                worker_busy = 0.0
            elif len(running) == 1:
                identifier = self.owned("", running[0], timeout=1)
                response = self.backend.request(
                    "GET",
                    "/containers/" + identifier + "/stats?stream=false&one-shot=true",
                    timeout=2,
                )
                if response.status != 200:
                    raise ValueError("Worker stats unavailable")
                total = strict_json(response.body)["cpu_stats"]["cpu_usage"]["total_usage"]
                if type(total) is not int or total < 0:
                    raise ValueError("Worker CPU counter is invalid")
                with self._capacity_state:
                    previous = self._worker_counters.get(identifier)
                    self._worker_counters[identifier] = total, now
                if previous and total >= previous[0] and now > previous[1]:
                    worker_busy = (total - previous[0]) / (1_000_000_000 * (now - previous[1]))
            self._last_capacity_error = 0
        except Exception:
            with self._capacity_state:
                self._worker_counters.clear()
                if not known_inventory:
                    running = list(self._applied_cpu)
            if self._last_capacity_error != 901:
                self.capacity_log(901)
                self._last_capacity_error = 901
        limit = self.cpu.observe(host, worker_busy, active_workers=len(running))
        update_ok = True
        for reference in running:
            try:
                identifier = self.owned("", reference, timeout=1)
                with self._capacity_state:
                    previous_limit = self._applied_cpu.get(identifier)
                if previous_limit is None or abs(previous_limit - limit) >= 0.01:
                    response = self.backend.request(
                        "POST",
                        "/containers/" + identifier + "/update",
                        encoded({"NanoCpus": round(limit * 1_000_000_000)}),
                        timeout=1,
                    )
                    if response.status != 200:
                        raise ValueError("Worker CPU update was not accepted")
                    with self._capacity_state:
                        self._applied_cpu[identifier] = limit
                    self.capacity_log(102, cpu_cores=round(limit, 4))
            except Exception:
                update_ok = False
                if self._last_capacity_error != 902:
                    self.capacity_log(902)
                    self._last_capacity_error = 902
        with self._capacity_state, self.cpu.lock:
            self.cpu.update_ok = update_ok
            self.cpu.applied_limit = max(self._applied_cpu.values(), default=0.0)

    def start_capacity(self) -> None:
        with self._capacity_lifecycle:
            if self._capacity_thread is not None and self._capacity_thread.is_alive():
                return
            self._capacity_stop.clear()
            self._capacity_wake.clear()

            def monitor():
                while not self._capacity_stop.is_set():
                    sampled_at = time.monotonic()
                    try:
                        self.capacity_tick()
                    except Exception:
                        self.cpu.observe(
                            capacity.HostSample(float(os.cpu_count() or 1), None),
                            None,
                            active_workers=0,
                        )
                        self.capacity_log(903)
                    self._capacity_wake.wait(max(0, 2 - (time.monotonic() - sampled_at)))
                    self._capacity_wake.clear()

            self._capacity_thread = threading.Thread(
                target=monitor, name="pot-capacity", daemon=True
            )
            self._capacity_thread.start()

    def stop_capacity(self) -> bool:
        self._capacity_stop.set()
        self._capacity_wake.set()
        thread = self._capacity_thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(5)
        return thread is None or not thread.is_alive()

    def route(self, method: str, target: str, body: bytes = b"") -> Response | Attach | Wait:
        if len(target) > 8192 or len(body) > BODY_LIMIT or any(ord(char) < 32 for char in target):
            raise Denied(413)
        parsed = urlsplit(target)
        if parsed.scheme or parsed.netloc or parsed.fragment or not parsed.path.startswith("/"):
            raise Denied()
        match = re.match(r"^(/v[0-9]{1,3}\.[0-9]{1,3})(?=/)", parsed.path)
        prefix = match[1] if match else ""
        path = parsed.path[len(prefix) :]
        try:
            entries = parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True)
            query = dict(entries)
            if len(query) != len(entries):
                raise ValueError("Repeated query key")
        except ValueError:
            raise Denied(400) from None
        if not prefix and path == "/pot/foreground" and method == "POST":
            value = strict_json(body)
            if query or not isinstance(value, dict) or set(value) != {"pending"}:
                raise Denied(400)
            try:
                self.cpu.foreground(value["pending"])
            except ValueError:
                raise Denied(400) from None
            self._capacity_wake.set()
            return Response(200, encoded(self.cpu.snapshot()), {"Content-Type": "application/json"})
        if not prefix and path == "/pot/capacity" and method == "GET":
            if query or body:
                raise Denied(400)
            return Response(200, encoded(self.cpu.snapshot()), {"Content-Type": "application/json"})
        if path in {"/_ping", "/version"} and method in (
            {"GET", "HEAD"} if path == "/_ping" else {"GET"}
        ):
            if query or body:
                raise Denied()
            return self.backend.request(method, prefix + path)
        if path.startswith("/images/") and path.endswith("/json") and method == "GET":
            if query or body or unquote(path[8:-5]) != self.policy.image:
                raise Denied()
            return self.backend.request(method, prefix + "/images/" + self.policy.image + "/json")
        if path == "/containers/json" and method == "GET":
            if body or set(query) - {"all", "limit", "size", "filters"}:
                raise Denied()
            filters = strict_json(query.get("filters", "{}").encode())
            if not isinstance(filters, dict) or set(filters) - {"label", "name"}:
                raise Denied()
            for key, values in list(filters.items()):
                if isinstance(values, dict) and all(value is True for value in values.values()):
                    values = list(values)
                    filters[key] = values
                if (
                    not isinstance(values, list)
                    or len(values) > 32
                    or any(
                        not isinstance(value, str) or len(value) > 256 or "\x00" in value
                        for value in values
                    )
                ):
                    raise Denied()
            values = [
                value
                for value in self.inventory(prefix, filters)
                if value.get("ImageID") == self.policy.image
            ]
            return Response(200, encoded(values), {"Content-Type": "application/json"})
        if path == "/containers/create" and method == "POST":
            if set(query) != {"name"} or not CONTAINER_NAME.fullmatch(query["name"]):
                raise Denied()
            value = strict_json(body)
            validate_create(self.policy, value)
            with self.creation_lock:
                # Stopped/unconfirmed and old-image workers also occupy the lane.
                if self.inventory(prefix):
                    raise Denied(409, "An owned worker must be removed before creating another")
                limit, allowed = self.cpu.plan()
                if not allowed:
                    raise Denied(503, "Host pressure defers isolated work")
                value["HostConfig"]["NanoCpus"] = round(limit * 1_000_000_000)
                response = self.backend.request(
                    method, prefix + path + "?" + urlencode(query), encoded(value)
                )
                if response.status == 201:
                    identifier = strict_json(response.body).get("Id")
                    if isinstance(identifier, str) and CONTAINER_ID.fullmatch(identifier):
                        with self._capacity_state:
                            self._applied_cpu[identifier] = limit
                    self.capacity_log(101, cpu_cores=round(limit, 4))
                return response
        match = re.fullmatch(r"/containers/([^/]+)(?:/(json|start|attach|wait))?", path)
        if not match or body:
            raise Denied()
        reference, action = match.groups()
        allowed = {
            ("GET", "json"): {"size"},
            ("POST", "start"): set(),
            ("POST", "attach"): {"logs", "stream", "stdin", "stdout", "stderr", "detachKeys"},
            ("POST", "wait"): {"condition"},
            ("DELETE", None): {"force", "v"},
        }
        if (method, action) not in allowed or set(query) - allowed[(method, action)]:
            raise Denied()
        if action == "attach":
            if (
                any(
                    query.get(key) not in {"1", "true"}
                    for key in ("stream", "stdin", "stdout", "stderr")
                )
                or query.get("logs", "0") not in {"0", "false"}
                or query.get("detachKeys", "") not in {"", "ctrl-p,ctrl-q"}
            ):
                raise Denied()
        elif action == "wait":
            if query.get("condition", "not-running") not in {"not-running", "next-exit", "removed"}:
                raise Denied()
        elif action == "json":
            if query.get("size", "0") not in {"0", "false"}:
                raise Denied()
        elif method == "DELETE":
            if query.get("force") not in {"1", "true"} or query.get("v", "0") not in {
                "0",
                "1",
                "false",
                "true",
            }:
                raise Denied()
        identifier = self.owned(prefix, reference)
        canonical = prefix + "/containers/" + identifier + ("/" + action if action else "")
        if query:
            canonical += "?" + urlencode(query)
        if action == "attach":
            return Attach(canonical)
        if action == "wait":
            return Wait(canonical)
        return self.backend.request(method, canonical)


def relay(downstream: socket.socket, upstream: socket.socket) -> None:
    def copy(source, destination):
        try:
            while data := source.recv(65536):
                destination.sendall(data)
        except OSError:
            pass
        finally:
            try:
                destination.shutdown(socket.SHUT_WR)
            except OSError:
                pass

    threads = [
        threading.Thread(target=copy, args=pair, daemon=True)
        for pair in ((downstream, upstream), (upstream, downstream))
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


class Handler(socketserver.StreamRequestHandler):
    server: Server
    rbufsize = 0  # No read-ahead may consume raw stdin sent after attach headers.
    wbufsize = 0

    def respond(self, response: Response, *, head=False):
        self.headers_sent = True
        reason = http.client.responses.get(response.status, "Docker response")
        headers = dict(response.headers or {})
        headers["Content-Length"] = str(len(response.body))
        headers["Connection"] = "close"
        self.wfile.write(
            (
                f"HTTP/1.1 {response.status} {reason}\r\n"
                + "".join(f"{key}: {value}\r\n" for key, value in headers.items())
                + "\r\n"
            ).encode("latin-1")
        )
        if not head:
            self.wfile.write(response.body)

    def respond_wait(self, target: str):
        connection, response = self.server.broker.backend.open_wait(target)
        try:
            # Docker CLI registers wait, waits for these headers, then starts the
            # container. Buffering its body until exit would deadlock startup.
            reason = http.client.responses.get(response.status, "Docker response")
            self.headers_sent = True
            self.wfile.write(
                (
                    f"HTTP/1.1 {response.status} {reason}\r\nContent-Type: application/json\r\n"
                    "Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
                ).encode("ascii")
            )
            total = 0
            while chunk := response.read(65536):
                total += len(chunk)
                if total > RESPONSE_LIMIT:
                    raise Denied(502, "Docker wait response exceeds its bound")
                self.wfile.write(f"{len(chunk):x}\r\n".encode() + chunk + b"\r\n")
            self.wfile.write(b"0\r\n\r\n")
        finally:
            connection.close()

    def handle(self):
        self.connection.settimeout(15)
        method = ""
        self.headers_sent = False
        try:
            line = self.rfile.readline(8193)
            if len(line) > 8192 or not line.endswith(b"\r\n"):
                raise Denied(400)
            method, target, version = line.decode("ascii").strip().split(" ")
            if version not in {"HTTP/1.1", "HTTP/1.0"}:
                raise Denied(400)
            headers, size = {}, len(line)
            while True:
                line = self.rfile.readline(16385)
                size += len(line)
                if size > 16384 or not line.endswith(b"\r\n"):
                    raise Denied(431)
                if line == b"\r\n":
                    break
                key, value = line.decode("latin-1").rstrip("\r\n").split(":", 1)
                key = key.lower()
                if not re.fullmatch(r"[a-z0-9-]+", key) or key in headers:
                    raise Denied(400)
                headers[key] = value.strip()
            if "transfer-encoding" in headers:
                raise Denied(400)
            length = headers.get("content-length", "0")
            if not re.fullmatch(r"[0-9]{1,9}", length) or int(length) > BODY_LIMIT:
                raise Denied(413)
            chunks, remaining = [], int(length)
            while remaining:
                chunk = self.rfile.read(min(65536, remaining))
                if not chunk:
                    raise Denied(400)
                chunks.append(chunk)
                remaining -= len(chunk)
            result = self.server.broker.route(method, target, b"".join(chunks))
            if isinstance(result, Wait):
                self.respond_wait(result.target)
            elif isinstance(result, Attach):
                with self.server.broker.backend.attach(result.target) as upstream:
                    raw = bytearray()
                    while b"\r\n\r\n" not in raw:
                        part = upstream.recv(4096)
                        if not part or len(raw) + len(part) > 65536:
                            raise Denied(502)
                        raw.extend(part)
                    header, initial = bytes(raw).split(b"\r\n\r\n", 1)
                    if header.split(b"\r\n", 1)[0].split(b" ", 2)[1] != b"101":
                        raise Denied(502, "Docker attach upgrade failed")
                    self.connection.settimeout(660)
                    self.headers_sent = True
                    self.wfile.write(header + b"\r\n\r\n" + initial)
                    relay(self.connection, upstream)
            else:
                self.respond(result, head=method == "HEAD")
        except Denied as error:
            if not self.headers_sent:
                self.respond(
                    Response(
                        error.status,
                        encoded({"message": error.message}),
                        {"Content-Type": "application/json"},
                    )
                )
        except (OSError, ValueError, http.client.HTTPException):
            try:
                if not self.headers_sent:
                    self.respond(
                        Response(502, encoded({"message": "Docker broker request failed"}))
                    )
            except OSError:
                pass


class Server(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True

    def __init__(self, path: str, broker: Broker):
        self.broker = broker
        self.capacity = threading.BoundedSemaphore(16)
        super().__init__(path, Handler)

    def process_request(self, request, client_address):
        if not self.capacity.acquire(blocking=False):
            request.close()
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self.capacity.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.capacity.release()

    def serve_forever(self, poll_interval=0.5):
        self.broker.start_capacity()
        try:
            super().serve_forever(poll_interval=poll_interval)
        finally:
            self.broker.stop_capacity()

    def server_close(self):
        self.broker.stop_capacity()
        super().server_close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", required=True)
    parser.add_argument("--upstream", default="/var/run/docker.sock")
    parser.add_argument("--image", required=True)
    parser.add_argument("--group-hash", required=True)
    parser.add_argument("--socket-gid", required=True, type=int)
    args = parser.parse_args()
    policy = Policy(args.image, args.group_hash)
    if (
        os.geteuid() != 0
        or args.socket_gid <= 0
        or not os.path.isabs(args.socket)
        or not os.path.isabs(args.upstream)
    ):
        parser.error("Run as root with absolute Unix socket paths and a dedicated non-root group")
    directory = os.path.dirname(args.socket)
    info = os.stat(directory, follow_symlinks=False)
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
        parser.error("The socket directory must be root-owned and not writable by group/others")
    if os.path.lexists(args.socket):
        info = os.lstat(args.socket)
        if not stat.S_ISSOCK(info.st_mode) or info.st_uid != 0:
            parser.error("Refusing to replace an unknown socket path")
        with socket.socket(socket.AF_UNIX) as probe:
            try:
                probe.connect(args.socket)
            except ConnectionRefusedError:
                os.unlink(args.socket)
            else:
                parser.error("The broker socket is already active")
    previous_umask = os.umask(0o117)
    try:
        with Server(args.socket, Broker(policy, Backend(args.upstream))) as server:
            os.chown(args.socket, 0, args.socket_gid)
            os.chmod(args.socket, 0o660)
            server.serve_forever(poll_interval=0.5)
    finally:
        os.umask(previous_umask)
        if os.path.lexists(args.socket) and stat.S_ISSOCK(os.lstat(args.socket).st_mode):
            os.unlink(args.socket)


if __name__ == "__main__":
    main()
