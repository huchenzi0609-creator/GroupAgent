from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path

import pytest

from groupagent.domain import ActorContext, DomainError, Role
from groupagent.sandbox import DockerSandbox, ProcessSpec, SandboxLimits
from groupagent.tools import ToolContext, ToolScope

IMAGE = os.environ.get("GROUPAGENT_WORKER_IMAGE", "")
HOST = os.environ.get("GROUPAGENT_DOCKER_HOST")


@pytest.fixture
def context(tmp_path: Path) -> ToolContext:
    actor = ActorContext(group_id="synthetic-group", member_id="synthetic-member", role=Role.MEMBER)
    return ToolContext(
        ToolScope(
            "synthetic-group",
            "synthetic-work",
            actor,
            tmp_path.resolve(),
            frozenset({"python_compute", "process_run", "shell_command"}),
            "synthetic-trace",
        ),
        "synthetic-call",
        "synthetic-effect",
        asyncio.Event(),
    )


def runner(**kwargs) -> DockerSandbox:
    return DockerSandbox(IMAGE, docker_host=HOST, **kwargs)


def test_digest_pin_and_container_configuration():
    with pytest.raises(ValueError, match="pinned"):
        DockerSandbox("python:latest")
    with pytest.raises(ValueError, match="Unix"):
        DockerSandbox("sha256:" + "a" * 64, docker_host="tcp://example.com:2375")
    sandbox = DockerSandbox("sha256:" + "a" * 64, docker_path="docker")
    args = sandbox._argv("synthetic-container")
    assert "--network=none" in args and "--read-only" in args
    assert "--security-opt=no-new-privileges:true" in args
    assert "--cap-drop=ALL" in args and "--pids-limit=64" in args
    assert "--mount" not in args and "--volume" not in args
    assert all("docker.sock" not in value for value in args)
    assert next(value for value in args if value.startswith("/work:")).startswith(
        "/work:rw,noexec,"
    )
    assert next(value for value in args if value.startswith("/tmp:")).startswith("/tmp:rw,noexec,")


async def test_missing_docker_never_falls_back_to_host(context):
    sandbox = DockerSandbox("sha256:" + "a" * 64, docker_path="/missing/synthetic-docker")
    with pytest.raises(DomainError) as error:
        await sandbox.run(
            ProcessSpec(program="python3", args=("-c", "print('no host run')")), context
        )
    assert error.value.code == "sandbox_unavailable"


async def test_failed_removal_never_acknowledges_container_quiescence(
    context, monkeypatch, tmp_path
):
    sandbox = DockerSandbox("sha256:" + "a" * 64, docker_path="synthetic-docker")
    name = "groupagent-" + "b" * 32
    sandbox.track_container(name, context.scope)

    async def unconfirmed(_home, *args, **kwargs):
        return (1, b"") if args[0] == "rm" else (0, (name + "\n").encode())

    monkeypatch.setattr(sandbox, "_control", unconfirmed)
    try:
        with pytest.raises(DomainError) as error:
            await sandbox.remove_container(tmp_path, name)
        assert error.value.code == "work_busy"
        assert DockerSandbox.scope_active(context.group_id, context.work_id)
    finally:
        DockerSandbox._managed.pop(name, None)


async def test_forbidden_executable_and_escaping_paths_rejected_before_docker(context):
    sandbox = DockerSandbox("sha256:" + "a" * 64, docker_path="/missing/synthetic-docker")
    for spec in (
        ProcessSpec(program="curl"),
        ProcessSpec(program="python3", input_paths=("../private.txt",)),
        ProcessSpec(program="python3", output_paths=("/private.txt",)),
    ):
        with pytest.raises(DomainError) as error:
            await sandbox.run(spec, context)
        assert error.value.code in {"permission_denied", "scope_violation"}


@pytest.mark.sandbox
@pytest.mark.skipif(not IMAGE, reason="Requires a real pinned local Docker worker image")
async def test_real_compute_input_output_stdin_and_no_host_mount(context):
    (context.workspace / "input.txt").write_text("synthetic input")
    sandbox = runner()
    info = await sandbox.preflight()
    assert info["os"] == "linux"
    result = await sandbox.run(
        ProcessSpec(
            program="python3",
            args=(
                "-I",
                "-c",
                "from pathlib import Path; import sys; Path('result.txt').write_text(Path('input.txt').read_text()+' '+sys.stdin.read()); print(6*7)",
            ),
            input_paths=("input.txt",),
            output_paths=("result.txt",),
            stdin="provided stdin",
        ),
        context,
    )
    assert result.succeeded and result.stdout.strip() == "42"
    assert (context.workspace / "result.txt").read_text() == "synthetic input provided stdin"
    assert result.outputs[0].path == "result.txt"
    assert (context.workspace / "input.txt").read_text() == "synthetic input"


@pytest.mark.sandbox
@pytest.mark.skipif(not IMAGE, reason="Requires a real pinned local Docker worker image")
async def test_real_identity_env_secret_files_and_network_boundaries(
    context, monkeypatch, tmp_path
):
    monkeypatch.setenv("DEEPSEEK_API_KEY", "SYNTHETIC_HOST_SECRET_CANARY")
    monkeypatch.setenv("GROUPAGENT_DATABASE_URL", "SYNTHETIC_DATABASE_SECRET_CANARY")
    canary = tmp_path / "not-staged.txt"
    canary.write_text("SYNTHETIC_FILE_CANARY")
    code = """
import json,os,socket
from pathlib import Path
observed={'uid':os.getuid(),'key_present':'DEEPSEEK_API_KEY' in os.environ,'dsn_present':'GROUPAGENT_DATABASE_URL' in os.environ,'docker_socket':Path('/var/run/docker.sock').exists(),'host_file':Path(HOST_FILE).exists()}
observed['caps']=[x for x in Path('/proc/self/status').read_text().splitlines() if x.startswith('CapEff:')][0].split()[-1]
observed['noexec_mounts'] = sorted(line.split()[1] for line in Path('/proc/mounts').read_text().splitlines() if line.split()[1] in {'/work','/tmp'} and 'noexec' in line.split()[3].split(','))
try:
    socket.create_connection(('93.184.216.34',443),timeout=1)
    observed['network_allowed']=True
except OSError:observed['network_allowed']=False
try:
    Path('/etc/groupagent-escape').write_text('forbidden')
    observed['root_write_allowed']=True
except OSError:observed['root_write_allowed']=False
try:
    Path('/proc/'+str(os.getppid())+'/environ').read_bytes()
    observed['supervisor_environment_readable']=True
except OSError:observed['supervisor_environment_readable']=False
print(json.dumps(observed))
""".replace("HOST_FILE", repr(str(canary)))
    result = await runner().run(ProcessSpec(program="python3", args=("-I", "-c", code)), context)
    assert result.succeeded
    observed = json.loads(result.stdout)
    assert observed == {
        "uid": 65532,
        "key_present": False,
        "dsn_present": False,
        "docker_socket": False,
        "host_file": False,
        "caps": "0000000000000000",
        "noexec_mounts": ["/tmp", "/work"],
        "network_allowed": False,
        "root_write_allowed": False,
        "supervisor_environment_readable": False,
    }
    assert "SYNTHETIC_HOST_SECRET_CANARY" not in result.model_dump_json()


@pytest.mark.sandbox
@pytest.mark.skipif(not IMAGE, reason="Requires a real pinned local Docker worker image")
async def test_real_timeout_and_output_caps(context):
    sandbox = runner()
    started = time.monotonic()
    result = await sandbox.run(
        ProcessSpec(
            program="python3",
            args=("-I", "-c", "import time; time.sleep(30)"),
            timeout_seconds=0.2,
        ),
        context,
    )
    assert result.timed_out and not result.outputs and time.monotonic() - started < 10
    result = await sandbox.run(
        ProcessSpec(
            program="python3",
            args=("-I", "-c", "print('x'*100000)"),
            max_output_bytes=1024,
        ),
        context,
    )
    assert result.output_limited and len(result.stdout.encode()) <= 1024 and not result.succeeded


@pytest.mark.sandbox
@pytest.mark.skipif(not IMAGE, reason="Requires a real pinned local Docker worker image")
async def test_real_cancellation_removes_process_and_suppresses_exports(context):
    sandbox = runner()
    task = asyncio.create_task(
        sandbox.run(
            ProcessSpec(
                program="python3",
                args=(
                    "-I",
                    "-c",
                    "import time; from pathlib import Path; time.sleep(30); Path('late.txt').write_text('late')",
                ),
                output_paths=("late.txt",),
            ),
            context,
        )
    )
    await asyncio.sleep(1)
    started = time.monotonic()
    context.cancelled.set()
    result = await asyncio.wait_for(task, 8)
    assert result.cancelled and time.monotonic() - started < 8
    assert not (context.workspace / "late.txt").exists()


@pytest.mark.sandbox
@pytest.mark.skipif(not IMAGE, reason="Requires a real pinned local Docker worker image")
async def test_real_cgroup_limits_and_memory_exhaustion(context):
    sandbox = runner(limits=SandboxLimits(memory_mb=256, workspace_mb=32, temp_mb=16, pids=32))
    code = "from pathlib import Path; import json; print(json.dumps({k:Path('/sys/fs/cgroup/'+k).read_text().strip() for k in ['memory.max','memory.swap.max','pids.max','cpu.max']}))"
    observation = await sandbox.run(
        ProcessSpec(program="python3", args=("-I", "-c", code)), context
    )
    assert observation.succeeded
    cgroup = json.loads(observation.stdout)
    assert cgroup["memory.max"] == str(256 * 1024 * 1024)
    assert cgroup["memory.swap.max"] == "0" and cgroup["pids.max"] == "32"
    assert cgroup["cpu.max"].split()[0] != "max"
    result = await sandbox.run(
        ProcessSpec(
            program="python3",
            args=("-I", "-c", "a=bytearray(1024*1024*1024); print(len(a))"),
            timeout_seconds=15,
        ),
        context,
    )
    assert not result.succeeded and result.exit_code != 0


@pytest.mark.sandbox
@pytest.mark.skipif(not IMAGE, reason="Requires a real pinned local Docker worker image")
async def test_real_workspace_quota_and_escaping_outputs(context):
    sandbox = runner(limits=SandboxLimits(workspace_mb=16, max_file_bytes=64 * 1024 * 1024))
    result = await sandbox.run(
        ProcessSpec(
            program="python3",
            args=(
                "-I",
                "-c",
                "from pathlib import Path; Path('big.bin').write_bytes(b'x'*(32*1024*1024))",
            ),
            output_paths=("big.bin",),
        ),
        context,
    )
    assert not result.succeeded and not (context.workspace / "big.bin").exists()
    refused = await sandbox.run(
        ProcessSpec(
            program="python3",
            args=(
                "-I",
                "-c",
                "from pathlib import Path; Path('escape').symlink_to('/etc/passwd')",
            ),
            output_paths=("escape",),
        ),
        context,
    )
    assert not refused.succeeded and refused.error_code == "output_contract_failed"
    assert refused.publication_state == "unpublished" and refused.outputs == ()
    assert len(refused.output_issues) == 1 and refused.output_issues[0].kind == "invalid_file"
    assert not (context.workspace / "escape").exists()


@pytest.mark.sandbox
@pytest.mark.skipif(not IMAGE, reason="Requires a real pinned local Docker worker image")
async def test_real_controller_pipe_loss_stops_child_without_late_exports(
    context, monkeypatch, tmp_path
):
    started = asyncio.Event()
    captured = []
    original = asyncio.create_subprocess_exec

    async def launch(*args, **kwargs):
        process = await original(*args, **kwargs)
        if "run" in args:
            captured.append((process, args[args.index("--name") + 1]))
            started.set()
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", launch)
    sandbox = runner()
    task = asyncio.create_task(
        sandbox.run(
            ProcessSpec(
                program="python3",
                args=(
                    "-I",
                    "-c",
                    "from pathlib import Path; import time; Path('started').write_text('synthetic'); time.sleep(30); Path('late.txt').write_text('late')",
                ),
                output_paths=("late.txt",),
            ),
            context,
        )
    )
    await started.wait()
    home = tmp_path / "empty-control-home"
    home.mkdir()
    process, name = captured[0]
    for _ in range(30):
        code, _ = await sandbox._control(
            home, "exec", "--user", "65532:65532", name, "/bin/test", "-f", "/work/started"
        )
        if code == 0:
            break
        await asyncio.sleep(0.05)
    assert code == 0
    process.stdin.close()
    result = await asyncio.wait_for(task, 8)
    assert result.cancelled and not result.outputs
    assert not (context.workspace / "late.txt").exists()
