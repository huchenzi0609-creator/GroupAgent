from __future__ import annotations

from types import SimpleNamespace

from groupagent.sandbox.supervisor import resource_usage


def counters(root, values):
    for name, value in values.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value)


def tmpfs(monkeypatch):
    monkeypatch.setattr(
        "groupagent.sandbox.supervisor.os.statvfs",
        lambda path: SimpleNamespace(
            f_blocks=10, f_bfree=4 if path == "/work" else 7, f_frsize=4096
        ),
    )


def test_v2_counters_keep_precedence_and_tmpfs_byte_accounting(tmp_path, monkeypatch):
    counters(
        tmp_path,
        {
            "memory.peak": "1048576\n",
            "pids.peak": "12\n",
            "pids.current": "2\n",
            "cpu.stat": "usage_usec 1234\nuser_usec 1000\nsystem_usec 234\n",
            "memory/memory.max_usage_in_bytes": "9999999\n",
            "cpu,cpuacct/cpuacct.usage": "9999999\n",
        },
    )
    tmpfs(monkeypatch)
    assert resource_usage(cgroup_root=tmp_path) == {
        "memory_peak_bytes": 1048576,
        "pids_peak": 12,
        "cpu_usage_usec": 1234,
        "workspace_used_bytes": 6 * 4096,
        "temp_used_bytes": 3 * 4096,
    }


def test_v1_peak_and_cpu_mounts_do_not_label_current_pids_as_peak(tmp_path, monkeypatch):
    counters(
        tmp_path,
        {
            "memory/memory.max_usage_in_bytes": "2097152\n",
            "cpu,cpuacct/cpuacct.usage": "2345678\n",
            "pids/pids.current": "7\n",
        },
    )
    tmpfs(monkeypatch)
    expected = {
        "memory_peak_bytes": 2097152,
        "cpu_usage_usec": 2345,
        "pids_current": 7,
        "workspace_used_bytes": 6 * 4096,
        "temp_used_bytes": 3 * 4096,
    }
    assert resource_usage(cgroup_root=tmp_path) == expected
    # Some cgroup v1 hosts mount cpuacct separately from cpu.
    (tmp_path / "cpu,cpuacct/cpuacct.usage").unlink()
    counters(tmp_path, {"cpuacct/cpuacct.usage": "2345678\n"})
    assert resource_usage(cgroup_root=tmp_path) == expected


def test_unreadable_or_malformed_os_counters_stay_absent(tmp_path, monkeypatch):
    counters(
        tmp_path,
        {
            "memory.peak": "not a counter",
            "memory/memory.max_usage_in_bytes": "-1",
            "cpu.stat": "usage_usec invalid\n",
            "cpu,cpuacct/cpuacct.usage": "-1000",
            "cpuacct/cpuacct.usage": "max",
            "pids.peak": "",
            "pids.current": "-2",
        },
    )

    def unavailable(path):
        raise PermissionError("Synthetic inaccessible worker mount")

    monkeypatch.setattr("groupagent.sandbox.supervisor.os.statvfs", unavailable)
    assert resource_usage(cgroup_root=tmp_path) == {}
