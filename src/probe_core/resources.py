"""Memory, CPU and disk limits for DuckDB, sized to what this process may
actually use: the container's cgroup limits when there are any, otherwise the
host's. Shared by the app (interactive queries, rasters) and the pipeline.

Moved here from data_lake/data_interface.py on 2026-09-18; data_interface
still re-exports these names.
"""

import os

import psutil


# DuckDB's own memory_limit only bounds ITS internal buffer pool -- it does
# NOT cover rows already pulled out via .df()/.fetchdf() into pandas, which
# is where an unbounded CALLER (e.g. accumulating many kacheln's worth of
# rows in a Python list across a big bbox loop) can still grow past whatever
# this limit says and take the whole host down regardless of it -- see
# derivate/maxi_ifk_and_raster.py's raster-kachel-count cap for the guard
# against THAT specific failure mode. Setting memory_limit here is still
# worth doing as a floor: it stops a single pathological query (a huge
# unbounded read/join) from ever growing DuckDB's OWN buffers past a safe
# share of the host's RAM -- whatever that host's RAM turns out to be
# (2026-08-16: an unconstrained raster job OOM-killed the entire WSL VM, not
# just the Streamlit process, on a 15 GB dev box -- this app has to stay
# safe on "whichever system it runs on", not just the box it was built on).
_DUCKDB_MEMORY_FRACTION = 0.25
_DUCKDB_MEMORY_LIMIT_MIN_BYTES = 512 * 1024 * 1024       # floor -- below this DuckDB can't do useful work at all
_DUCKDB_MEMORY_LIMIT_MAX_BYTES = 4 * 1024 * 1024 * 1024  # ceiling -- several connections can coexist (one per session/thread), so no single one gets to claim an unbounded share


def container_memory_limit_bytes() -> int:
    """RAM this process may actually use: the cgroup limit when running in a
    container, otherwise the host's physical RAM.

    psutil.virtual_memory().total reports the NODE's RAM inside a container,
    not the pod's limit. Measured in the Hosttech rehearsal 2026-09-16: psutil
    saw 16.8 GB while /sys/fs/cgroup/memory.max said 8.6 GB, which sized
    max_raster_kacheln() at 159 kacheln where the pod's own limit implies ~82.
    Sizing guards off the node is how a pod gets to accept work it cannot
    survive -- see NOTES.md, rehearsal finding 3."""
    host_total = psutil.virtual_memory().total
    for path in ('/sys/fs/cgroup/memory.max',                     # cgroup v2
                 '/sys/fs/cgroup/memory/memory.limit_in_bytes'):  # cgroup v1
        try:
            raw = open(path).read().strip()
        except OSError:
            continue
        if raw == 'max':          # v2 spelling for "no limit"
            break
        try:
            limit = int(raw)
        except ValueError:
            continue
        # v1 reports a huge sentinel when unlimited; anything >= host RAM is
        # not a real constraint either way
        if 0 < limit < host_total:
            return limit
        break
    return host_total


def container_cpu_limit() -> int:
    """CPUs this process may actually use: the cgroup quota when running in a
    container, otherwise os.cpu_count(). Same reasoning as
    container_memory_limit_bytes -- os.cpu_count() sees the node's 8 CPUs
    while the pod's cpu.max allowed 4, and oversubscribing threads multiplies
    DuckDB's concurrent on-disk spill."""
    try:
        quota, period = open('/sys/fs/cgroup/cpu.max').read().split()
        if quota != 'max':
            return max(1, int(int(quota) / int(period)))
    except (OSError, ValueError):
        pass
    return os.cpu_count() or 1


def duckdb_max_temp_directory_size() -> str | None:
    """Cap for DuckDB's on-disk spill, from PROBE_DUCKDB_MAX_TEMP_SIZE
    (e.g. '50GB'), or None to leave DuckDB's own default alone.

    DuckDB defaults to "90% of available disk space", which inside a pod means
    90% of the NODE's filesystem: it cannot see an emptyDir's sizeLimit, so it
    spills until the kubelet evicts the pod. Any k8s deployment should set this
    env var to match the volume it writes into."""
    return os.getenv('PROBE_DUCKDB_MAX_TEMP_SIZE') or None


def safe_duckdb_memory_limit() -> str:
    """DuckDB memory_limit string (e.g. '2048MB'), sized to a safe fraction
    of the RAM this process may actually use and clamped to [512MB, 4GB] --
    so the same code behaves safely on an 8 GB laptop, a 64 GB server, or a
    memory-capped container alike, instead of a value tuned for one box."""
    total = container_memory_limit_bytes()
    budget = min(max(int(total * _DUCKDB_MEMORY_FRACTION), _DUCKDB_MEMORY_LIMIT_MIN_BYTES),
                 _DUCKDB_MEMORY_LIMIT_MAX_BYTES)
    return f"{budget // (1024 * 1024)}MB"
