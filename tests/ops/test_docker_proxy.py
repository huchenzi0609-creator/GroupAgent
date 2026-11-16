from __future__ import annotations

import copy
import importlib.util
import json
import socket
import sys
import threading
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest

SPEC = importlib.util.spec_from_file_location(
    "pot_docker_proxy_tests", Path(__file__).resolve().parents[2] / "deploy/pot_docker_proxy.py"
)
proxy = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = proxy
SPEC.loader.exec_module(proxy)
IMAGE = "sha256:" + "a" * 64
GROUP = "b" * 64
IDENTIFIER = "c" * 64
NAME = "groupagent-" + "d" * 32
POLICY = proxy.Policy(IMAGE, GROUP)


def create_body():
    # Includes Docker CLI's actual zero/default encodings captured against a
    # synthetic Unix HTTP server, not a live Docker daemon.
    return {
        "Image": IMAGE,
        "User": "0:0",
        "WorkingDir": "/work",
        "AttachStdin": True,
        "AttachStdout": True,
        "AttachStderr": True,
        "OpenStdin": True,
        "StdinOnce": True,
        "Cmd": None,
        "Entrypoint": None,
        "Env": None,
        "Labels": {
            "org.groupagent.managed": "1",
            "org.groupagent.group": GROUP,
            "org.groupagent.scope": "e" * 64,
        },
        "NetworkingConfig": {"EndpointsConfig": {}},
        "HostConfig": {
            "NetworkMode": "none",
            "ReadonlyRootfs": True,
            "Init": True,
            "Memory": proxy.MEMORY,
            "MemorySwap": proxy.MEMORY,
            "NanoCpus": 750_000_000,
            "CpuShares": 128,
            "OomScoreAdj": 500,
            "PidsLimit": 192,
            "Tmpfs": copy.deepcopy(proxy.TMPFS),
            "SecurityOpt": ["no-new-privileges:true"],
            "CapDrop": ["ALL"],
            "CapAdd": ["CAP_" + name for name in sorted(proxy.CAPS)],
            "Ulimits": [{"Name": "nofile", "Hard": 256, "Soft": 256}],
            "Privileged": False,
            "MemorySwappiness": -1,
            "Cgroup": "",
            "BlkioWeightDevice": [],
            "BlkioDeviceReadBps": [],
            "BlkioDeviceWriteBps": [],
            "BlkioDeviceReadIOps": [],
            "BlkioDeviceWriteIOps": [],
        },
    }


class Backend:
    def __init__(self):
        self.calls, self.containers = [], []
        self.inspection = {
            "Id": IDENTIFIER,
            "Image": IMAGE,
            "Config": {"Labels": {"org.groupagent.managed": "1", "org.groupagent.group": GROUP}},
        }

    def request(self, method, target, body=b"", **kwargs):
        self.calls.append((method, target, body))
        path = urlsplit(target).path
        if path.endswith("/containers/json"):
            return proxy.Response(200, proxy.encoded(self.containers))
        if "/containers/" in path and path.endswith("/json"):
            return proxy.Response(200, proxy.encoded(self.inspection))
        if path.endswith("/containers/create"):
            self.containers.append(
                {"Id": IDENTIFIER, "ImageID": IMAGE, "Labels": json.loads(body)["Labels"]}
            )
            return proxy.Response(201, proxy.encoded({"Id": IDENTIFIER, "Warnings": []}))
        return proxy.Response(204)


@pytest.mark.parametrize(
    "path,value",
    [
        (("Image",), "sha256:" + "f" * 64),
        (("Cmd",), ["sh"]),
        (("Entrypoint",), ["/bin/sh"]),
        (("Env",), ["LD_PRELOAD=/tmp/x"]),
        (("HostConfig", "Privileged"), True),
        (("HostConfig", "NetworkMode"), "host"),
        (("HostConfig", "PidMode"), "host"),
        (("HostConfig", "IpcMode"), "host"),
        (("HostConfig", "Binds"), ["/:/host"]),
        (("HostConfig", "Mounts"), [{"Type": "bind", "Source": "/", "Target": "/host"}]),
        (("HostConfig", "Devices"), [{"PathOnHost": "/dev/mem", "PathInContainer": "/dev/mem"}]),
        (("HostConfig", "Memory"), 0),
        (("HostConfig", "MemorySwap"), -1),
        (("HostConfig", "NanoCpus"), 2_000_000_000),
        (("HostConfig", "PidsLimit"), -1),
        (("HostConfig", "CapAdd"), ["CAP_SYS_ADMIN"]),
        (("HostConfig", "CapDrop"), []),
        (("HostConfig", "SecurityOpt"), ["seccomp=unconfined"]),
        (("HostConfig", "ReadonlyRootfs"), False),
        (("HostConfig", "ReadonlyPaths"), []),
        (("HostConfig", "MaskedPaths"), []),
        (("HostConfig", "CgroupParent"), "/game"),
        (("HostConfig", "RestartPolicy"), {"Name": "always"}),
        (("HostConfig", "Tmpfs"), {"/tmp": "rw,exec,size=2g"}),
        (("HostConfig", "UnknownFutureCapability"), None),
        (("Labels", "org.groupagent.group"), "f" * 64),
        (("NetworkingConfig",), {"EndpointsConfig": {"bridge": {}}}),
    ],
)
def test_create_rejects_privilege_mount_resource_and_identity_changes(path, value):
    body = create_body()
    selected = body
    for key in path[:-1]:
        selected = selected[key]
    selected[path[-1]] = value
    backend = Backend()
    with pytest.raises(proxy.Denied):
        proxy.Broker(POLICY, backend).route(
            "POST", "/v1.52/containers/create?name=" + NAME, proxy.encoded(body)
        )
    assert backend.calls == []


def test_exact_cli_body_passes_and_second_worker_is_rejected():
    backend = Backend()
    broker = proxy.Broker(POLICY, backend)
    assert (
        broker.route(
            "POST", "/v1.52/containers/create?name=" + NAME, proxy.encoded(create_body())
        ).status
        == 201
    )
    with pytest.raises(proxy.Denied) as error:
        broker.route("POST", "/v1.52/containers/create?name=" + NAME, proxy.encoded(create_body()))
    assert error.value.status == 409
    assert len([call for call in backend.calls if call[0] == "POST"]) == 1


@pytest.mark.parametrize(
    "method,path",
    [
        ("POST", "/containers/" + NAME + "/start"),
        ("POST", "/containers/" + NAME + "/wait?condition=next-exit"),
        ("DELETE", "/containers/" + NAME + "?force=1&v=1"),
        ("POST", "/containers/" + NAME + "/attach?stream=1&stdin=1&stdout=1&stderr=1"),
        ("GET", "/containers/" + NAME + "/json"),
    ],
)
@pytest.mark.parametrize("mismatch", ["image", "group", "managed"])
def test_foreign_container_never_reaches_an_operation(method, path, mismatch):
    backend = Backend()
    if mismatch == "image":
        backend.inspection["Image"] = "sha256:" + "f" * 64
    else:
        backend.inspection["Config"]["Labels"]["org.groupagent." + mismatch] = "foreign"
    with pytest.raises(proxy.Denied):
        proxy.Broker(POLICY, backend).route(method, "/v1.52" + path)
    assert len(backend.calls) == 1 and backend.calls[0][0] == "GET"
    assert backend.calls[0][1].endswith("/json")


def test_owned_operation_uses_inspected_id_and_lists_force_ownership():
    backend = Backend()
    broker = proxy.Broker(POLICY, backend)
    broker.route("DELETE", "/v1.52/containers/" + NAME + "?force=1&v=1")
    assert backend.calls[-1][1] == "/v1.52/containers/" + IDENTIFIER + "?force=1&v=1"
    backend.containers = [
        {
            "ImageID": IMAGE,
            "Labels": {"org.groupagent.managed": "1", "org.groupagent.group": GROUP},
        },
        {
            "ImageID": IMAGE,
            "Labels": {"org.groupagent.managed": "1", "org.groupagent.group": "foreign"},
        },
        {
            "ImageID": "other-image",
            "Labels": {"org.groupagent.managed": "1", "org.groupagent.group": GROUP},
        },
    ]
    result = broker.route("GET", "/v1.52/containers/json?all=1")
    assert len(json.loads(result.body)) == 1
    query = parse_qs(urlsplit(backend.calls[-1][1]).query)
    assert set(json.loads(query["filters"][0])["label"]) == {
        "org.groupagent.managed=1",
        "org.groupagent.group=" + GROUP,
    }


@pytest.mark.parametrize(
    "method,path",
    [
        ("POST", "/images/create?fromImage=alpine"),
        ("POST", "/build"),
        ("GET", "/info"),
        ("GET", "/events"),
        ("GET", "/volumes"),
        ("POST", "/networks/create"),
        ("POST", "/containers/" + NAME + "/exec"),
        ("POST", "/exec/" + IDENTIFIER + "/start"),
        ("POST", "/containers/" + NAME + "/kill"),
        ("GET", "/containers/" + NAME + "/archive"),
        ("GET", "/images/other/json"),
        ("GET", "/containers/%2f..%2fjson/json"),
        ("GET", "/containers/" + IDENTIFIER + "/stats?stream=false"),
        ("POST", "/containers/" + IDENTIFIER + "/update"),
    ],
)
def test_every_other_endpoint_is_denied(method, path):
    backend = Backend()
    with pytest.raises(proxy.Denied):
        proxy.Broker(POLICY, backend).route(method, "/v1.52" + path)
    assert backend.calls == []


def test_duplicate_json_and_oversized_body_do_not_reach_daemon():
    with pytest.raises(proxy.Denied):
        proxy.strict_json(b'{"Image":"one","Image":"two"}')
    backend = Backend()
    with pytest.raises(proxy.Denied) as error:
        proxy.Broker(POLICY, backend).route(
            "POST", "/containers/create?name=" + NAME, b"x" * (proxy.BODY_LIMIT + 1)
        )
    assert error.value.status == 413 and backend.calls == []


def test_http101_preserves_early_stdin_and_bidirectional_raw_stream():
    downstream, client = socket.socketpair()
    upstream, daemon = socket.socketpair()
    for connection in (downstream, client, upstream, daemon):
        connection.settimeout(2)
    backend = Backend()
    backend.attach = lambda _target: upstream
    errors = []
    received = []
    payload, output = b'{"synthetic":"stdin"}\n\x00binary', b"\x01\x00\x00\x00\x00\x00\x00\x04done"

    def serving():
        try:
            proxy.Handler(
                downstream, "local", SimpleNamespace(broker=proxy.Broker(POLICY, backend))
            )
        except BaseException as error:
            errors.append(error)
        finally:
            downstream.close()

    def daemon_reply():
        try:
            daemon.sendall(
                b"HTTP/1.1 101 UPGRADED\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n\r\n" + output
            )
            data = bytearray()
            while chunk := daemon.recv(65536):
                data.extend(chunk)
            received.append(bytes(data))
            daemon.shutdown(socket.SHUT_WR)
        except BaseException as error:
            errors.append(error)
        finally:
            daemon.close()

    threads = [threading.Thread(target=serving), threading.Thread(target=daemon_reply)]
    for thread in threads:
        thread.start()
    try:
        request = (
            "POST /v1.52/containers/"
            + NAME
            + "/attach?stream=1&stdin=1&stdout=1&stderr=1 HTTP/1.1\r\nHost: docker\r\nContent-Length: 0\r\n\r\n"
        ).encode()
        client.sendall(request + payload)  # Stdin deliberately arrives before HTTP 101.
        client.shutdown(socket.SHUT_WR)
        result = bytearray()
        while chunk := client.recv(65536):
            result.extend(chunk)
        assert bytes(result).startswith(b"HTTP/1.1 101") and bytes(result).endswith(output)
    finally:
        client.close()
        for thread in threads:
            thread.join(3)
    assert not errors and received == [payload]
    assert all(not thread.is_alive() for thread in threads)


def test_wait_headers_are_forwarded_before_container_exit_body():
    downstream, client = socket.socketpair()
    client.settimeout(2)
    finished = threading.Event()
    backend = Backend()

    class PendingWait:
        status = 200
        sent = False

        def read(self, size):
            assert finished.wait(2)
            if self.sent:
                return b""
            self.sent = True
            return b'{"StatusCode":0,"Error":null}'

    backend.open_wait = lambda _target: (SimpleNamespace(close=lambda: None), PendingWait())

    def serve():
        try:
            proxy.Handler(
                downstream, "local", SimpleNamespace(broker=proxy.Broker(POLICY, backend))
            )
        finally:
            downstream.close()

    thread = threading.Thread(target=serve)
    thread.start()
    try:
        client.sendall(
            (
                "POST /v1.52/containers/"
                + NAME
                + "/wait?condition=next-exit HTTP/1.1\r\nHost: docker\r\nContent-Length: 0\r\n\r\n"
            ).encode()
        )
        data = bytearray()
        while b"\r\n\r\n" not in data:
            data.extend(client.recv(4096))
        assert bytes(data).startswith(b"HTTP/1.1 200") and not finished.is_set()
        finished.set()  # Model Docker CLI starting the worker after wait registration.
        while chunk := client.recv(4096):
            data.extend(chunk)
        assert b'{"StatusCode":0,"Error":null}' in data and data.endswith(b"0\r\n\r\n")
    finally:
        finished.set()
        client.close()
        thread.join(3)
    assert not thread.is_alive()


def test_cpu_policy_boosts_idle_without_feedback_and_drops_busy_immediately():
    clock = [0.0]
    control = proxy.capacity.CapacityControl(clock=lambda: clock[0])
    control.foreground(0)
    assert control.observe(proxy.capacity.HostSample(2, 0), 0, active_workers=1) == 0.5
    clock[0] = 4
    control.foreground(0)
    assert control.observe(proxy.capacity.HostSample(2, 0), 0, active_workers=1) == 1.5
    clock[0] = 6
    control.foreground(0)
    # The worker itself consumes 1.4 cores. Only .1 external core is busy.
    assert control.observe(proxy.capacity.HostSample(2, 1.5), 1.4, active_workers=1) == 1.5
    assert control.snapshot()["external_busy_cores"] == 0.1
    clock[0] = 6.1
    # External work spikes; no four-second delay or EWMA lag for a decrease.
    assert control.observe(proxy.capacity.HostSample(2, 1.7), 0.2, active_workers=1) == 0.25
    clock[0] = 8
    control.foreground(0)
    assert control.observe(proxy.capacity.HostSample(2, 0), 0, active_workers=1) == 0.25


def test_foreground_timeout_unknown_stats_and_pressure_are_conservative():
    clock = [0.0]
    control = proxy.capacity.CapacityControl(clock=lambda: clock[0])
    control.foreground(0)
    clock[0] = 4
    control.observe(proxy.capacity.HostSample(2, 0), 0, active_workers=1)
    assert control.plan() == (1.5, True)
    control.foreground(2)
    assert control.plan() == (0.5, True)  # The HTTP signal itself clamps immediately.
    clock[0] = 8
    control.foreground(0)
    assert control.observe(proxy.capacity.HostSample(2, 0), 0, active_workers=1) == 1.5
    clock[0] = 14
    assert control.plan() == (0.5, True)  # Stale signal/sample cannot retain a boost.
    control.foreground(0)
    assert control.observe(proxy.capacity.HostSample(2, 0), None, active_workers=1) == 0.5
    assert control.snapshot()["stats_ok"] == 0
    assert (
        control.observe(
            proxy.capacity.HostSample(2, 0, available_mb=64, pressure=True), 0, active_workers=1
        )
        == 0.25
    )
    assert control.plan() == (0.25, False)


def test_host_sampler_uses_host_deltas_and_swap_pressure(tmp_path):
    (tmp_path / "stat").write_text("cpu 20 0 10 60 10 0 0 0\ncpu0 0\ncpu1 0\n")
    (tmp_path / "meminfo").write_text("MemAvailable: 512000 kB\n")
    (tmp_path / "vmstat").write_text("pswpin 0\npswpout 0\n")
    sampler = proxy.capacity.HostSampler(tmp_path)
    assert sampler.sample(0).busy is None
    (tmp_path / "stat").write_text("cpu 40 0 20 130 10 0 0 0\ncpu0 0\ncpu1 0\n")
    (tmp_path / "vmstat").write_text("pswpin 0\npswpout 8192\n")
    sample = sampler.sample(2)
    assert sample.cores == 2 and sample.busy == pytest.approx(0.6)
    assert sample.available_mb == 500 and sample.swap_mb_per_second > 4 and sample.pressure


@pytest.mark.parametrize("value", [True, -1, 1001, 0.5, "0", None])
def test_foreground_endpoint_rejects_invalid_counts_without_backend_access(value):
    backend = Backend()
    with pytest.raises(proxy.Denied):
        proxy.Broker(POLICY, backend).route(
            "POST", "/pot/foreground", proxy.encoded({"pending": value})
        )
    assert backend.calls == []


def test_capacity_control_endpoints_are_numeric_and_create_only_rewrites_cpu():
    backend = Backend()
    broker = proxy.Broker(POLICY, backend)
    result = broker.route("POST", "/pot/foreground", b'{"pending":1}')
    assert all(type(value) in (int, float) for value in json.loads(result.body).values())
    assert broker._capacity_wake.is_set() and not backend.calls
    broker.route("POST", "/containers/create?name=" + NAME, proxy.encoded(create_body()))
    forwarded = json.loads(backend.calls[-1][2])
    expected = create_body()
    expected["HostConfig"]["NanoCpus"] = 500_000_000
    assert forwarded == expected
    assert json.loads(broker.route("GET", "/pot/capacity").body)["foreground_pending"] == 1


def test_internal_stats_and_updates_validate_owner_and_never_change_other_limits():
    class AdaptiveBackend(Backend):
        def __init__(self):
            super().__init__()
            self.total, self.failed = 0, False
            self.containers = [
                {
                    "Id": IDENTIFIER,
                    "ImageID": IMAGE,
                    "State": "running",
                    "Labels": self.inspection["Config"]["Labels"],
                }
            ]

        def request(self, method, target, body=b"", **kwargs):
            if "/stats?" in target:
                self.calls.append((method, target, body))
                return (
                    proxy.Response(503)
                    if self.failed
                    else proxy.Response(
                        200,
                        proxy.encoded({"cpu_stats": {"cpu_usage": {"total_usage": self.total}}}),
                    )
                )
            if target.endswith("/update"):
                self.calls.append((method, target, body))
                return proxy.Response(200)
            return super().request(method, target, body, **kwargs)

    clock, host_busy = [0.0], [0.6]
    backend = AdaptiveBackend()
    broker = proxy.Broker(POLICY, backend)
    broker.cpu = proxy.capacity.CapacityControl(clock=lambda: clock[0])
    broker.host_sampler = SimpleNamespace(
        sample=lambda _: proxy.capacity.HostSample(2, host_busy[0])
    )
    for instant, total in [(0, 0), (2, 1_000_000_000), (4, 2_000_000_000)]:
        clock[0], backend.total = instant, total
        broker.cpu.foreground(0)
        broker.capacity_tick()
    assert broker.cpu.snapshot()["applied_cpu_cores"] == 1.5
    clock[0], backend.total, host_busy[0] = 6, 5_000_000_000, 1.6
    broker.cpu.foreground(0)
    broker.capacity_tick()
    assert broker.cpu.snapshot()["cpu_limit_cores"] == 1.5
    backend.failed, clock[0] = True, 8
    broker.capacity_tick()
    assert broker.cpu.snapshot()["applied_cpu_cores"] == 0.5
    updates = [call for call in backend.calls if call[1].endswith("/update")]
    assert updates and all(set(json.loads(call[2])) == {"NanoCpus"} for call in updates)
    assert all(call[1] == "/containers/" + IDENTIFIER + "/update" for call in updates)
    # The internal path does not make either endpoint available to the client.
    with pytest.raises(proxy.Denied):
        broker.route("POST", "/containers/" + IDENTIFIER + "/update", b'{"NanoCpus":1500000000}')


def test_capacity_thread_start_is_idempotent_and_stop_joins_it():
    broker = proxy.Broker(POLICY, Backend())
    broker.start_capacity()
    first = broker._capacity_thread
    try:
        broker.start_capacity()
        assert broker._capacity_thread is first and first.is_alive()
    finally:
        assert broker.stop_capacity()
    assert not first.is_alive()


def test_confirmed_host_pressure_refuses_create_before_daemon_mutation():
    backend = Backend()
    broker = proxy.Broker(POLICY, backend)
    broker.cpu.observe(
        proxy.capacity.HostSample(2, 0, available_mb=64, pressure=True), 0, active_workers=0
    )
    with pytest.raises(proxy.Denied) as error:
        broker.route("POST", "/containers/create?name=" + NAME, proxy.encoded(create_body()))
    assert error.value.status == 503
    assert all(method == "GET" for method, _, _ in backend.calls)
    assert broker.cpu.snapshot()["create_allowed"] == 0
