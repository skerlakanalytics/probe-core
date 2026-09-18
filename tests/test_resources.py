"""Container-aware sizing, with the cgroup files faked."""

import io
from types import SimpleNamespace

import pytest

from probe_core import resources

GiB = 1024**3


@pytest.fixture
def cgroup(monkeypatch):
    """Serve fake /sys/fs/cgroup files; a missing entry behaves like a missing file."""
    files = {}

    def fake_open(path, *args, **kwargs):
        if path in files:
            return io.StringIO(files[path])
        raise OSError(path)

    monkeypatch.setattr(resources, "open", fake_open, raising=False)
    monkeypatch.setattr(resources.psutil, "virtual_memory", lambda: SimpleNamespace(total=16 * GiB))
    monkeypatch.setattr(resources.os, "cpu_count", lambda: 8)
    return files


def test_memory_limit_cgroup_v2(cgroup):
    cgroup["/sys/fs/cgroup/memory.max"] = str(8 * GiB)
    assert resources.container_memory_limit_bytes() == 8 * GiB


def test_memory_unlimited_v2_uses_host(cgroup):
    cgroup["/sys/fs/cgroup/memory.max"] = "max"
    assert resources.container_memory_limit_bytes() == 16 * GiB


def test_memory_limit_cgroup_v1_sentinel_uses_host(cgroup):
    cgroup["/sys/fs/cgroup/memory/memory.limit_in_bytes"] = str(2**63 - 4096)
    assert resources.container_memory_limit_bytes() == 16 * GiB


def test_memory_no_cgroup_uses_host(cgroup):
    assert resources.container_memory_limit_bytes() == 16 * GiB


def test_cpu_quota(cgroup):
    cgroup["/sys/fs/cgroup/cpu.max"] = "400000 100000"
    assert resources.container_cpu_limit() == 4


def test_cpu_fractional_quota_rounds_down_but_at_least_one(cgroup):
    cgroup["/sys/fs/cgroup/cpu.max"] = "50000 100000"
    assert resources.container_cpu_limit() == 1


def test_cpu_unlimited_uses_host(cgroup):
    cgroup["/sys/fs/cgroup/cpu.max"] = "max 100000"
    assert resources.container_cpu_limit() == 8


@pytest.mark.parametrize("limit, expected", [
    (8 * GiB, "2048MB"),      # 25 % of the Bedag pod
    (1 * GiB, "512MB"),       # floor
])
def test_safe_duckdb_memory_limit(cgroup, limit, expected):
    cgroup["/sys/fs/cgroup/memory.max"] = str(limit)
    assert resources.safe_duckdb_memory_limit() == expected


def test_safe_duckdb_memory_limit_ceiling(cgroup, monkeypatch):
    monkeypatch.setattr(resources.psutil, "virtual_memory", lambda: SimpleNamespace(total=64 * GiB))
    assert resources.safe_duckdb_memory_limit() == "4096MB"


def test_max_temp_directory_size(monkeypatch):
    monkeypatch.delenv("PROBE_DUCKDB_MAX_TEMP_SIZE", raising=False)
    assert resources.duckdb_max_temp_directory_size() is None
    monkeypatch.setenv("PROBE_DUCKDB_MAX_TEMP_SIZE", "90GiB")
    assert resources.duckdb_max_temp_directory_size() == "90GiB"
