"""Read-side access to the data lake: id_kachel spatial math, Data-Lake-Silver
scans (backing derivate scripts), and Data-Lake-Gold/catalog access (backing
pgr-atlas's Streamlit app). No build/write logic lives here — that's the
*_worker.py pipeline modules in workers/. Anything that only needs schema
*facts* (folder names, column types, constants) belongs in data_lake_schema.py
instead; this module is for the read *code* that those facts get used by, so
a script can import "how do I read the lake" without pulling in a build
pipeline's ray/duckdb-heavy machinery.

Consolidated here 2026-08-04 from three previously-separate copies:
- id_kachel encode/decode existed independently in workers/data_lake_gold_worker.py,
  derivate/maxi_ifk_and_raster.py, and monitoring/probe_progress_service.py
  before an earlier pass moved it here.
- The GOLD/CATALOG sections (EVENT/PIXEL/AREA patterns, catalog stats) were
  pgr-atlas's OWN data_interface.py, a near-identical module duplicated
  across repos. Merged in so there is exactly one data-lake read layer;
  pgr-atlas's app.py now imports this module (at first via sys.path, same
  pattern already used for probe_control_center/derivate/maxi_event_export).
  kacheln_touching_bbox() and kacheln_in_bbox() were the same bbox->kacheln
  computation under two names with swapped argument order — unified as
  kacheln_in_bbox(xmin, ymin, xmax, ymax) (shapely/geopandas' own bbox
  convention), the name+order pgr-atlas's call sites already used.

Two local, non-gold data sources feed the GOLD/CATALOG sections below:
- The event manifest (config/app.yaml event_manifest) is the
  upfront simulation plan — every anriss's position/area/depth-variant,
  independent of whether it has been simulated yet. Backs CATALOG.
- The fixed physics parameter grid (config/app.yaml physics_grid) —
  applied uniformly to every anriss, also not gold-derived.

Gold itself (the actual simulation OUTPUT) only ever lives on S3 —
s3://maxi/Data-Lake-Gold/SIM_SPATIAL/id_kachel=NNNNNNNN/data.parquet, partially
filled while the campaign runs. Raw columns only, no probabilities yet
(future enrich phase). Every function that reads gold content goes through
S3, backed by a local manifest (refresh_gold_manifest) that tracks which
kacheln are finalized and raises GoldNotReadyError for anything not yet
complete.

Design rules for the GOLD/CATALOG half (carried over from the app's own copy,
still worth keeping now this is a shared module):
- No Streamlit imports. UI concerns (caching decorators, spinners, progress
  bars) live in the app; long-running functions take an optional
  progress_callback instead.
- Coordinates crossing this interface are LV95 (EPSG:2056); the only
  exceptions return WGS84 explicitly for web-map payloads and say so.
"""

import itertools
import json
import math
import os
import shutil
import threading
from functools import lru_cache
from pathlib import Path

import duckdb
import geopandas as gpd
import pandas as pd
from pyproj import Transformer
from shapely.geometry import box

from probe_core.campaign import (
    EVENT_CATALOG_KEY, EVENT_CATALOG_STATS_KEY, EVENT_MANIFEST_KEY, EVENTS_PER_BATCH, PHYSICS_GRID,
)
from probe_core.data_lake.data_lake_schema import (
    BATCH_SILVER_S3_SUFFIX, DATA_LAKE_DIR_SILVER, DATA_LAKE_DIR_GOLD_SIM_SPATIAL,
    DATA_LAKE_DIR_GOLD_SIM_ANRISS, DATA_LAKE_DIR_DERIVATE_HULLS, GOLD_MAX_REACH_M,
    BATCH_RANGE_SIZE, batch_range_prefix,
)
# Sizing helpers moved to probe_core.resources; re-exported here because both
# the app and the pipeline import them from this module.
from probe_core.resources import (  # noqa: F401
    container_cpu_limit, container_memory_limit_bytes,
    duckdb_max_temp_directory_size, safe_duckdb_memory_limit,
)
from probe_core.s3 import configure_s3_for_duckdb, get_s3_client

# ── CONFIGURATION ────────────────────────────────────────────────────────────
CRS_LV95 = "EPSG:2056"

# Physics grid: probe_core.campaign.PHYSICS_GRID, applied uniformly to every
# anriss -- not gold-derived, but needed to enumerate every (mu, xsi, tau0)
# combination a given anriss will eventually get.
_maxi_params = PHYSICS_GRID
_PHYSICS_GRID = list(itertools.product(_maxi_params["mu"], _maxi_params["xsi"], _maxi_params["tau0"]))

# EVENTS_PER_BATCH (probe_core.campaign) -- same value the pipeline's
# workers/batch_worker.py batch_id_expr uses ((sort_key - 1) // EVENTS_PER_BATCH + 1) --
# needed by _hull_fragment_s3_key below to compute an id_anriss's batch/range
# straight from arithmetic, matching the campaign's actual batching.

# The one bucket a deployment reads everything from (gold, catalog, hulls,
# derivates, Gebäudeschatten, tilesets): maxi = full canton, a test bucket =
# a region-scoped copy. PROBE_S3_BUCKET_GOLD before 0.7.0.
S3_BUCKET_GOLD = os.getenv("PROBE_S3_BUCKET", "maxi")
# Upfront simulation plan (not gold -- it exists before any simulation runs):
# the 540 MB, 122.6M-row manifest at the gold bucket's root (a copy of the
# fleet's input/ original; re-copy it to both gold buckets if it is ever
# re-issued). Physically sorted by sort_key (SW->NE batch sweep), so a
# per-anriss `WHERE id_anriss = ?` against it can't skip row groups -- this
# module never reads it; per-anriss lookups go to the event catalog below.
EVENT_MANIFEST_S3_URI = f"s3://{S3_BUCKET_GOLD}/{EVENT_MANIFEST_KEY}"

# Event catalog (2026-09-27, replaces the local catalog.db): the manifest's
# rows re-sorted by id_anriss (id_anriss, id_prozessquelle, x_anriss,
# y_anriss, d, A, h, h_type, sort_key) with small row groups, so a lookup
# reads the footer (cached per process) and one row group; plus a small JSON
# of precomputed totals (manifest rows, distinct anrisse/areas/prozessquellen,
# map centre, per home-kachel counts and values). Both are built once by
# ProBE_control_center's data_lake/build_event_catalog.py -- rebuild them
# whenever the manifest changes.
EVENT_CATALOG_S3_URI = f"s3://{S3_BUCKET_GOLD}/{EVENT_CATALOG_KEY}"

# All reads in this module are against SIM_SPATIAL (bbox/pixel access) — the
# SIM_ANRISS cousin (data_lake/build_silver_to_gold_anriss.py) has no reader here
# yet, that's a separate addition once it's actually built and populated.
GOLD_S3_ROOT   = f"s3://{S3_BUCKET_GOLD}/{DATA_LAKE_DIR_GOLD_SIM_SPATIAL}"

_GOLD_COLUMNS = ['id_kachel', 'id_prozessquelle', 'id_anriss', 'x_anriss', 'y_anriss',
                 'A', 'd', 'h', 'mu', 'xsi', 'tau0', 'x', 'y',
                 'Fliesstiefe', 'Fliessgeschwindigkeit', 'Druck']


# ── EXCEPTIONS ────────────────────────────────────────────────────────────────

class GoldNotReadyError(Exception):
    """Raised when a query touches one or more gold kacheln that aren't
    finalized yet, per the local manifest (call refresh_gold_manifest() to
    update it). Distinct from an empty DataFrame, which means the relevant
    kacheln ARE finalized but genuinely contain no matching rows."""
    def __init__(self, pending_kacheln):
        self.pending_kacheln = sorted(pending_kacheln)
        super().__init__(
            f"{len(self.pending_kacheln)} kachel(n) not yet finalized "
            f"(or manifest is stale — try refresh_gold_manifest() first): "
            f"{self.pending_kacheln}")


class HullFragmentNotIndexedError(Exception):
    """Raised when id_anriss's covering 1000-batch range has no hull fragment
    yet (derivate/build_hull_fragments.py hasn't scattered it — its range
    isn't Range-Done yet, or the hulls loop hasn't caught up). Deliberately
    no fallback to a coarser bbox (decision 2026-08-05): every EVENT-pattern
    read now goes through get_anriss_hull_fragment()'s exact per-anriss bbox
    via _event_relevant_kacheln, so this blocks the whole event view, not
    just the hull display, until the fragment lands."""
    def __init__(self, id_anriss):
        self.id_anriss = id_anriss
        super().__init__(
            f"id_anriss {id_anriss}: no hull fragment yet (its range hasn't "
            f"been scattered by derivate/build_hull_fragments.py) — not indexed yet")


class SimAnrissNotIndexedError(Exception):
    """Raised when id_anriss's covering 1000-batch range has no SIM_ANRISS
    fragment yet (data_lake/build_silver_to_gold_anriss.py hasn't scattered it —
    its range isn't Range-Done yet, or that loop hasn't caught up). Same
    "no fallback, block until it lands" decision as
    HullFragmentNotIndexedError — no fallback to SIM_SPATIAL here either,
    callers that want that behavior should catch this and call
    get_anriss_all_scenarios_gold_data() themselves."""
    def __init__(self, id_anriss):
        self.id_anriss = id_anriss
        super().__init__(
            f"id_anriss {id_anriss}: no SIM_ANRISS fragment yet (its range hasn't "
            f"been scattered by data_lake/build_silver_to_gold_anriss.py) — not indexed yet")


# ── id_kachel spatial math (GOLD_HIVE_KEY grid: id_kachel = e*10000 + n) ───────
KACHEL_SQL = "(floor(x / 1000)::INTEGER * 10000 + floor(y / 1000)::INTEGER)"
FINALIZE_RING = math.ceil(GOLD_MAX_REACH_M / 1000.0)  # e.g. 2 -> 5x5 neighborhood


def kachel_neighbors(id_kachel, ring=FINALIZE_RING):
    """id_kachel -> list of the (2*ring+1)^2 neighborhood kachel ids (incl.
    itself). Conservative/fixed-ring: use this when only the anriss's kachel
    is known, not its true pixel reach."""
    e, n = id_kachel // 10000, id_kachel % 10000
    return [(e + de) * 10000 + (n + dn)
            for de in range(-ring, ring + 1) for dn in range(-ring, ring + 1)]


def kacheln_in_bbox(xmin: float, ymin: float, xmax: float, ymax: float) -> list[int]:
    """All id_kachel integers whose 1 km LV95 tile overlaps the bbox — the
    exact-extent counterpart to kachel_neighbors()'s fixed ring: use this
    when an event's true reach is already known (e.g. an anriss's own
    xmin/xmax/ymin/ymax, or a GOLD_MAX_REACH_M-padded box) instead of the
    conservative ring."""
    return [e * 10000 + n
            for e in range(int(xmin // 1000), int(xmax // 1000) + 1)
            for n in range(int(ymin // 1000), int(ymax // 1000) + 1)]


# ── Data-Lake-Silver reads (derivate scripts) ─────────────────────────────────

# Process silver in bounded chunks rather than one unbounded S3 glob + GROUP
# BY. Measured 2026-08-04: 93,017 silver files, 397 GB, at only ~7.6% of the
# campaign's ~1.23M target batches -- projects to ~5.2 TB at completion. An
# unbatched pass over that is the same failure mode that crashed the WSL box
# in pgr-atlas's scripts/tilesets/overview_preprocessing.py (real disk space, not just
# memory -- see that script's module docstring), and this connection doesn't
# set memory_limit/temp_directory either.
#
# Chunking is safe with NO final cross-chunk merge, unlike a spatial batching
# scheme: one silver batch file's id_anriss values never appear in any OTHER
# batch file (a batch = 100 distinct events/anrisse; that batch's full
# param-combo + LEAD sim set all lands in that one file), so each chunk's
# GROUP BY dedup is already globally correct on its own.
TARGET_BATCH_FILES = 1000

# The real ceiling is the Windows host's C: drive (the WSL virtual disk lives
# on it as a file) -- `df` on the WSL side reports the virtual disk's own
# headroom, not the real constraint. Duplicated from
# pgr-atlas's scripts/tilesets/overview_preprocessing.py (separate repos, not imported).
_WINDOWS_HOST_MOUNT = Path("/mnt/c")
MIN_FREE_GB_TO_START = 5.0
MIN_FREE_GB_PER_BATCH = 1.0


def _real_free_gb() -> float | None:
    """Real free space on the Windows host, not the WSL virtual disk. None
    outside WSL (no /mnt/c)."""
    if not _WINDOWS_HOST_MOUNT.is_dir():
        return None
    return shutil.disk_usage(_WINDOWS_HOST_MOUNT).free / 1e9


def _check_disk_space(min_free_gb: float, context: str) -> None:
    free_gb = _real_free_gb()
    if free_gb is not None and free_gb < min_free_gb:
        raise RuntimeError(
            f"Only {free_gb:.1f} GB free on the Windows host drive (checked "
            f"{_WINDOWS_HOST_MOUNT}), need >= {min_free_gb:.1f} GB {context}. "
            "Free up space before retrying -- see CLAUDE.md WSL crash notes."
        )


def build_pixel_cache(con, bucket, cache_dir, limit=None):
    """Scan Data-Lake-Silver -> local (id_anriss, x, y) pixel cache,
    deduplicated across every parameter combo of the same anriss (mu/xsi/tau0,
    plus the 2 thickness/relTh variants each anriss gets). con needs
    configure_s3_for_duckdb (utils.py) already applied.

    Batched by silver file (TARGET_BATCH_FILES at a time, see its comment
    above) -- cache_dir ends up holding one parquet file per batch; read it
    as a glob (compute_bboxes / hull_geometry), never as a single file.

    limit: stop once at least this many distinct id_anriss have been
    written, checked after each batch -- safe to check only there (not
    mid-batch) since one id_anriss's rows never span two batch files (see
    TARGET_BATCH_FILES's comment), so a batch boundary can't split an
    anriss's count. Also shrinks the batch size while a limit is active, so
    a smoke test with e.g. limit=500 doesn't still have to scan a full
    TARGET_BATCH_FILES=1000 worth of files (~100k anrisse) before the first
    check even happens. None (default): process the whole corpus."""
    cache_dir = Path(cache_dir)
    _check_disk_space(MIN_FREE_GB_TO_START, "to start the pixel-cache scan")

    pattern = f"s3://{bucket}/{DATA_LAKE_DIR_SILVER}/*{BATCH_SILVER_S3_SUFFIX}"
    files = [r[0] for r in con.execute(f"SELECT file FROM glob('{pattern}')").fetchall()]
    batch_size = TARGET_BATCH_FILES if limit is None else min(TARGET_BATCH_FILES, 20)
    print(f"{len(files):,} silver files to scan"
          + (f" (stopping early once >= {limit:,} id_anriss found)" if limit else ""))

    if cache_dir.exists():
        shutil.rmtree(cache_dir)
    cache_dir.mkdir(parents=True)

    n_batches = (len(files) + batch_size - 1) // batch_size
    n_anriss = 0
    for i in range(n_batches):
        batch_files = files[i * batch_size:(i + 1) * batch_size]
        _check_disk_space(MIN_FREE_GB_PER_BATCH, f"before batch {i + 1}/{n_batches}")
        batch_path = cache_dir / f"batch_{i + 1:04d}.parquet"
        file_list_sql = ", ".join(f"'{f}'" for f in batch_files)
        con.execute(f"""
            COPY (
                SELECT id_anriss, x, y
                FROM read_parquet([{file_list_sql}])
                GROUP BY id_anriss, x, y
            ) TO '{batch_path}' (FORMAT PARQUET)
        """)
        batch_n_anriss = con.execute(
            f"SELECT COUNT(DISTINCT id_anriss) FROM read_parquet('{batch_path}')"
        ).fetchone()[0]
        n_anriss += batch_n_anriss
        print(f"  batch {i + 1}/{n_batches}: {len(batch_files)} files -> "
              f"{batch_path.stat().st_size / 1e6:.1f} MB, {n_anriss:,} id_anriss so far")
        if limit and n_anriss >= limit:
            print(f"  reached limit ({n_anriss:,} >= {limit:,}) -- stopping early")
            break


def compute_bboxes(con, cache_dir):
    """Per-id_anriss xmin/xmax/ymin/ymax (float32, matching silver's own x/y
    type) from a build_pixel_cache() cache directory."""
    return con.execute(f"""
        SELECT id_anriss,
               MIN(x)::FLOAT AS xmin, MAX(x)::FLOAT AS xmax,
               MIN(y)::FLOAT AS ymin, MAX(y)::FLOAT AS ymax
        FROM read_parquet('{cache_dir}/*.parquet')
        GROUP BY id_anriss
    """).df()


# ── SHARED GOLD/CATALOG HELPERS (spatial math + S3/manifest plumbing) ─────────

def _empty_gold_df() -> pd.DataFrame:
    return pd.DataFrame(columns=_GOLD_COLUMNS)


def _s3_gold_connection() -> duckdb.DuckDBPyConnection:
    """Fresh DuckDB connection configured for the gold lake's S3 bucket.
    Deliberately NOT reused/cached across calls: every extraction call pays
    full connection+auth setup, so performance measurements reflect the
    real per-call cost, not a warmed-up shortcut."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute(f"SET memory_limit='{safe_duckdb_memory_limit()}'")
    configure_s3_for_duckdb(con)
    return con


def _s3_derivate_connection() -> duckdb.DuckDBPyConnection:
    """Fresh DuckDB connection for reading Data-Lake-Derivate/ fragments —
    separate from _s3_gold_connection() so the spatial extension (needed to
    pull the hull polygon out of the fragment's GeoParquet geometry column)
    doesn't load on every plain gold-row read, which never touches geometry."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute(f"SET memory_limit='{safe_duckdb_memory_limit()}'")
    configure_s3_for_duckdb(con)
    return con


def _thread_local_cursor(tls: threading.local, base_con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
    """One duckdb cursor per thread, duplicated from base_con via
    con.cursor() -- a plain duckdb.Connection is NOT safe for concurrent
    access from multiple threads (confirmed 2026-08-11: segfaults/bus
    errors even under pure concurrent reads, no write involved at all),
    but con.cursor() duplicates sharing the same underlying database are.
    Needed because Streamlit runs each browser session's script on its own
    thread within one server process, so any LOCAL (non-S3) query in this
    module can be hit by multiple users' threads at the same moment --
    every caller of a cached singleton connection (_catalog_con) must go
    through this instead of using that connection
    object's .execute() directly."""
    con = getattr(tls, "con", None)
    if con is None:
        con = base_con.cursor()
        tls.con = con
    return con


# Finalized gold kacheln, held in memory (2026-09-27, replaces the local
# gold_manifest.db). One S3 listing fills it; the app does that once at
# startup (the MAXI campaign is finished and gold is immutable). Unlike the old
# "only ever add" database it is a fresh snapshot of S3 on every refresh, so a
# rebuilt lake can no longer leave phantom kacheln behind.
_finalized: frozenset[int] | None = None
_finalized_lock = threading.Lock()


def _finalized_kacheln() -> frozenset[int]:
    """Kacheln finalized on S3 as of the last refresh_gold_manifest() --
    refreshes on first use."""
    if _finalized is None:
        refresh_gold_manifest()
    return _finalized


def refresh_gold_manifest(progress_callback=None) -> dict:
    """Lists the production gold lake on S3 (directory listing only, no
    parquet data read) and replaces the in-memory set of finalized kacheln.

    Returns {'new_kacheln': int, 'total_kacheln': int} -- new = not in the
    previous snapshot of this process."""
    global _finalized
    s3 = _s3_gold_connection()
    live_files = s3.execute(f"SELECT file FROM glob('{GOLD_S3_ROOT}/id_kachel=*/data.parquet')").fetchall()
    live = frozenset(int(f[0].split('id_kachel=')[1].split('/')[0]) for f in live_files)
    with _finalized_lock:
        new = len(live - (_finalized or frozenset()))
        _finalized = live
    if progress_callback:
        progress_callback(new, new, f"{new:,} neue Kacheln")
    return {'new_kacheln': new, 'total_kacheln': len(live)}


# ── EVENT — everything for one anriss/event (simulation detail view) ─────────

def _hull_fragment_s3_key(id_anriss: int) -> str | None:
    """id_anriss -> its exact Data-Lake-Derivate/anriss_umhuellende/ fragment
    key, straight from arithmetic -- no S3 access. A fragment covers one
    1000-batch range (build_hull_fragments.py); which range an id_anriss
    falls into is fully determined by its manifest sort_key (SW->NE batch
    sweep, same expr workers/batch_worker.py's create_batch_query uses to
    assign batch IDs: batch_id = (sort_key - 1) // EVENTS_PER_BATCH + 1,
    range_id = batch_id // BATCH_RANGE_SIZE, matching data_lake_schema.py's
    batch_range_id/batch_range_prefix and build_hull_fragments.py's
    range_batch_ids exactly).

    Reads sort_key from the id_anriss-sorted event catalog on S3, not the raw
    manifest: the manifest is sorted by sort_key, so a per-call
    `WHERE id_anriss = ?` against it can't prune row groups and ends up
    scanning most of the 540MB file.

    Returns None if id_anriss isn't in the manifest at all -- callers should
    fall back to the old glob-scan lookup rather than treat that as
    "not yet scattered", since it may mean the batch-math assumption above
    doesn't hold for this row (defense in depth, not expected in practice --
    id_anriss_exists() already gates every caller before this point)."""
    sort_key = _catalog_sort_key(id_anriss)
    if sort_key is None:
        return None
    batch_id = (sort_key - 1) // EVENTS_PER_BATCH + 1
    range_id = batch_id // BATCH_RANGE_SIZE
    return f"{DATA_LAKE_DIR_DERIVATE_HULLS}/{batch_range_prefix(range_id)}_umhuellende.parquet"


def _hull_fragment_row(con, source: str, id_anriss: int, geometry: bool):
    """geometry=False skips the umhuellende polygon column entirely --
    see _hull_fragment_lookup for why that's the expensive part."""
    geom_select = (
        ", ST_AsGeoJSON(ST_Transform(umhuellende, 'EPSG:2056', 'EPSG:4326', always_xy := true))"
        if geometry else ""
    )
    return con.execute(
        f"""SELECT id_prozessquelle, xmin, xmax, ymin, ymax{geom_select}
            FROM read_parquet('{source}')
            WHERE id_anriss = ?""",
        [int(id_anriss)],
    ).fetchone()


def _hull_fragment_lookup(id_anriss: int, geometry: bool):
    """Shared targeted-file-then-glob-fallback read behind both
    get_anriss_hull_fragment (geometry=True, for map display) and
    _anriss_bbox (geometry=False, for _event_relevant_kacheln's gold-read
    gating, which never needs the polygon).

    Reads exactly ONE fragment file, keyed via _hull_fragment_s3_key (2026-
    08-11) rather than glob+filter over every fragment (~145 files today,
    ~1,227 at full campaign size). Measured against production S3: a 404 on
    the single targeted key is what now means "not scattered yet" (caught
    below), replacing the old "no row came back from the glob" signal.
    Falls back to the full glob scan if the key can't be computed at all
    (id_anriss missing from the manifest -- see _hull_fragment_s3_key) or if
    the targeted file exists but doesn't contain the row (would mean the
    batch-math assumption broke somewhere; correctness over speed).

    geometry=False also uses a plain httpfs connection (_s3_gold_connection)
    instead of _s3_derivate_connection, skipping the spatial extension load
    entirely -- there's no ST_* call to justify it on this path. Measured
    2026-08-11 on the same single targeted file: ~0.7s for the 4 bbox
    columns vs. ~14-20s once the umhuellende column is also projected --
    Parquet row-group stats on id_anriss can't skip anything within a
    fragment (not sorted by id_anriss), so ANY row matching means decoding
    that column across the whole file/row-group before the WHERE filter
    drops it back to one row; the 4 plain-double bbox columns are cheap to
    decode that way, the polygon (WKB) column isn't.

    Raises HullFragmentNotIndexedError if id_anriss's covering range hasn't
    been scattered yet — no fallback to the coarser square (decision
    2026-08-05)."""
    con = _s3_derivate_connection() if geometry else _s3_gold_connection()
    key = _hull_fragment_s3_key(id_anriss)
    row = None
    if key is not None:
        try:
            row = _hull_fragment_row(con, f"s3://{S3_BUCKET_GOLD}/{key}", id_anriss, geometry)
        except duckdb.HTTPException as e:
            if "404" not in str(e):
                raise
            raise HullFragmentNotIndexedError(id_anriss) from None

    if row is None:
        # Fallback: id_anriss wasn't in the manifest (key is None) or the
        # targeted file didn't have it (batch-math mismatch) -- either way,
        # the old exhaustive lookup is still correct, just slow.
        glob = f"s3://{S3_BUCKET_GOLD}/{DATA_LAKE_DIR_DERIVATE_HULLS}/*.parquet"
        row = _hull_fragment_row(con, glob, id_anriss, geometry)
        if row is None:
            raise HullFragmentNotIndexedError(id_anriss)
    return row


@lru_cache(maxsize=4096)
def get_anriss_hull_fragment(id_anriss: int) -> dict:
    """S3: one id_anriss's precomputed derivate row from
    Data-Lake-Derivate/anriss_umhuellende/ (derivate/build_hull_fragments.py)
    — its EXACT xmin/xmax/ymin/ymax bbox across every silver pixel and every
    parameter combo ever simulated for it, plus the hull polygon (as GeoJSON,
    for map display). Tight vs. the old GOLD_MAX_REACH_M-padded square: still
    an upper bound on any ONE parameter combo's footprint (built from every
    combo pooled together), but usually far smaller than a fixed 3000m box.

    Only ever called for the map-display use (pgr-atlas's app.py:
    precomputed_hull_cache) — _event_relevant_kacheln (gold-read gating,
    the other historical caller) goes through the much cheaper _anriss_bbox
    instead (2026-08-11 split, see _hull_fragment_lookup's docstring for the
    ~0.7s vs ~14-20s measurement that motivated it). Still worth its own
    @lru_cache: within one Anriss-open, app.py itself can re-render/rerun
    several times (parameter dropdown changes etc.) before a session-level
    cache (st.session_state.precomputed_hull_cache) would otherwise dedupe
    it, and this makes repeat calls free at the process level too.
    HullFragmentNotIndexedError is NOT cached by lru_cache (only successful
    returns are), so a not-yet-scattered id_anriss keeps getting re-checked
    on every call, cheaply (~0.26s, a plain 404), as the campaign
    progresses. Bounded at 4096 entries (small dicts each) since this is a
    long-running server process.

    hull_geojson is the one WGS84 exception this module's coordinate rule
    calls for (see module docstring) — the fragment's own geometry column is
    LV95 like everything else here, reprojected on the way out since this
    value is only ever consumed as a web-map overlay (same ST_Transform(...,
    always_xy := true) pattern as catalog_overview_stats)."""
    id_prozessquelle, xmin, xmax, ymin, ymax, hull_geojson = _hull_fragment_lookup(id_anriss, geometry=True)
    return {
        "id_prozessquelle": id_prozessquelle,
        "xmin": xmin, "xmax": xmax, "ymin": ymin, "ymax": ymax,
        "hull_geojson": json.loads(hull_geojson),
    }


@lru_cache(maxsize=4096)
def _anriss_bbox(id_anriss: int) -> tuple[float, float, float, float]:
    """id_anriss -> (xmin, ymin, xmax, ymax) only — the cheap half of
    get_anriss_hull_fragment (see _hull_fragment_lookup), for
    _event_relevant_kacheln's gold-read gating, which never renders the
    hull polygon.

    Cached independently of get_anriss_hull_fragment, not derived from it —
    deliberately simple (no cross-cache peeking) rather than checking
    whether get_anriss_hull_fragment already has this id_anriss cached and
    reusing its bbox for free. In pgr-atlas's app.py, the actual call order
    today (get_anriss_hull_fragment always runs first, for the map overlay,
    before _event_relevant_kacheln needs a bbox), that means this pays for
    its own ~0.7s S3 read even though the answer was technically already in
    hand — a small, deliberately-accepted cost in exchange for not adding a
    second cache that has to stay in sync with the first. Any caller that
    only ever needs the bbox (e.g. a bulk export script with no map) still
    never pays for the polygon at all, which was the actual goal."""
    _id_pq, xmin, xmax, ymin, ymax = _hull_fragment_lookup(id_anriss, geometry=False)
    return (xmin, ymin, xmax, ymax)


def _event_relevant_kacheln(id_anriss: int) -> tuple[list[int], tuple[float, float, float, float]]:
    """Every kachel one anriss's data can actually reach — sourced from its
    own hull-fragment bbox (_anriss_bbox) rather than a generic center +/-
    GOLD_MAX_REACH_M square (replaced 2026-08-05). id_kachel is a
    deterministic function of x/y (kacheln_in_bbox), so this is the exact set
    to read, no maintained per-anriss index needed.

    Uses _anriss_bbox, not get_anriss_hull_fragment (2026-08-11 split) —
    this never needs the hull polygon, only its bbox, and the polygon is
    the expensive part of that lookup (see _hull_fragment_lookup).

    No fallback if the fragment isn't built yet — _anriss_bbox raises
    HullFragmentNotIndexedError straight through, which blocks
    get_anriss_all_scenarios_gold_data for that anriss until its range is
    scattered, not just the hull display.

    Also returns the fragment's own bbox — callers pass it back into their
    read_parquet WHERE clause as a bbox predicate on x/y (the leading sort
    columns of gold SIM), alongside the exact id_anriss match. This bbox is
    the anriss's real reach (not a fixed 3000m square), so it prunes row
    groups far harder than the old approach did, on top of touching fewer
    kacheln in the first place."""
    bbox = _anriss_bbox(id_anriss)
    candidates = kacheln_in_bbox(*bbox)
    finalized = _finalized_kacheln()
    pending = [k for k in candidates if k not in finalized]
    if pending:
        raise GoldNotReadyError(pending)
    return candidates, bbox


def get_anriss_all_scenarios_gold_data(id_anriss: int) -> pd.DataFrame:
    """S3: raw gold-lake rows for EVERY simulated parameter combination of
    one anriss (used for the cross-scenario footprint envelope, and as the
    single wide read pgr-atlas's app.py filters client-side per
    A/h/mu/xsi/tau0 dropdown change rather than re-reading S3 per combo —
    see its anriss_all_scenarios_cache).

    See _event_relevant_kacheln for the completeness gating (raises
    GoldNotReadyError if any reachable kachel isn't finalized yet,
    HullFragmentNotIndexedError if id_anriss's hull fragment isn't built
    yet). All relevant kacheln are read in ONE read_parquet(file_list) call
    (same batching pattern as refresh_gold_manifest) rather than one
    sequential call per kachel — an anriss can still touch multiple kacheln
    near a tile boundary even with the tight per-anriss bbox, and a Python
    loop of single-file calls paid that many full S3 round trips one after
    another instead of letting DuckDB fetch/scan them concurrently via its
    own I/O threads. The bbox predicate (x/y, the leading sort columns) is
    there purely to help row-group pruning inside each file — id_anriss
    alone (3rd sort key) can't prune well, x/y bounds can; the exact
    id_anriss match still does the real filtering, the bbox can only ever
    be a superset of it."""
    relevant, (xmin, ymin, xmax, ymax) = _event_relevant_kacheln(id_anriss)
    if not relevant:
        return _empty_gold_df()

    files = [f"{GOLD_S3_ROOT}/id_kachel={k}/data.parquet" for k in relevant]
    con = _s3_gold_connection()
    return con.execute(
        "SELECT * FROM read_parquet(?) WHERE x BETWEEN ? AND ? AND y BETWEEN ? AND ? AND id_anriss = ?",
        [files, xmin, xmax, ymin, ymax, int(id_anriss)],
    ).df()


def _sim_anriss_s3_key(id_anriss: int) -> str | None:
    """id_anriss -> its exact Data-Lake-Gold/SIM_ANRISS/ fragment key,
    straight from arithmetic — no S3 access. Identical sort_key/batch-range
    math to _hull_fragment_s3_key (both derivates share the same batch-range
    keying, see data_lake/build_silver_to_gold_anriss.py's module docstring), just
    a different DATA_LAKE_DIR_* root and filename suffix.

    Returns None if id_anriss isn't in the manifest at all — same "defense
    in depth, not expected in practice" caveat as _hull_fragment_s3_key.
    Reads sort_key from the event catalog, not the raw manifest -- see
    _hull_fragment_s3_key's docstring for why."""
    sort_key = _catalog_sort_key(id_anriss)
    if sort_key is None:
        return None
    batch_id = (sort_key - 1) // EVENTS_PER_BATCH + 1
    range_id = batch_id // BATCH_RANGE_SIZE
    return f"{DATA_LAKE_DIR_GOLD_SIM_ANRISS}/{batch_range_prefix(range_id)}_sim_anriss.parquet"


def _sim_anriss_rows(con, source: str, id_anriss: int) -> pd.DataFrame:
    return con.execute(
        f"SELECT * FROM read_parquet('{source}') WHERE id_anriss = ?",
        [int(id_anriss)],
    ).df()


def get_anriss_sim_data(id_anriss: int) -> pd.DataFrame:
    """S3: every Gold-SIM row for one id_anriss (every parameter combo,
    every pixel) from the id_anriss-sorted cousin lake (SIM_ANRISS) — a
    single targeted small-file read (data_lake/build_silver_to_gold_anriss.py),
    not a spatial kachel scan. Same data, same columns, as
    get_anriss_all_scenarios_gold_data() — just a different, much cheaper
    backend for this exact access pattern (measured 2026-08-12: ~34x less
    data, ~5x faster on a sample kachel, growing to ~76x for large-footprint
    events — see docs/claude-memory/project_gold_row_group_size_benchmark.md).

    Wired into pgr-atlas's app.py as the primary path (try this first,
    catch SimAnrissNotIndexedError and fall back to
    get_anriss_all_scenarios_gold_data() for an anriss whose range isn't
    scattered yet) — confirmed still current 2026-08-25, see
    docs/claude-memory/project_gold_hilbert_access_benchmark.md.

    Targeted-key-then-glob-fallback, same defensive pattern as
    _hull_fragment_lookup: a 404 on the single targeted key means "not
    scattered yet" (raises SimAnrissNotIndexedError); a targeted file that
    exists but doesn't contain the row falls back to the full glob scan
    (would mean the batch-math assumption broke somewhere — correctness
    over speed, matching the hull-fragment lookup's own reasoning).

    Raises SimAnrissNotIndexedError if id_anriss's covering range hasn't
    been scattered yet — no fallback to SIM_SPATIAL (decision mirrors
    get_anriss_hull_fragment's "no fallback to the coarser square"; a
    caller that wants that behavior can catch this and call
    get_anriss_all_scenarios_gold_data() itself)."""
    con = _s3_gold_connection()
    key = _sim_anriss_s3_key(id_anriss)
    df = None
    if key is not None:
        try:
            candidate = _sim_anriss_rows(con, f"s3://{S3_BUCKET_GOLD}/{key}", id_anriss)
            if not candidate.empty:
                df = candidate
        except duckdb.HTTPException as e:
            if "404" not in str(e):
                raise
            raise SimAnrissNotIndexedError(id_anriss) from None

    if df is None:
        glob = f"s3://{S3_BUCKET_GOLD}/{DATA_LAKE_DIR_GOLD_SIM_ANRISS}/*.parquet"
        df = _sim_anriss_rows(con, glob, id_anriss)
        if df.empty:
            raise SimAnrissNotIndexedError(id_anriss)
    return df


def get_event_params(id_anriss: int) -> pd.DataFrame:
    """LOCAL (event manifest x physics grid, not gold): every parameter
    combination the campaign will (eventually) simulate for this anriss —
    not filtered by what has actually landed in gold yet. Columns match
    gold's names: A, h, d, mu, xsi, tau0, plus x_anriss/y_anriss/h_type.

    Reads the id_anriss-sorted event catalog on S3 rather than the 540MB
    manifest, which is ordered by sort_key (SW->NE batch sweep) and so
    can't prune row groups for a WHERE id_anriss = ?."""
    rows = _catalog_rows(int(id_anriss)).drop(columns="sort_key")
    if rows.empty:
        raise ValueError(f"id_anriss {id_anriss} not in the event manifest")
    grid = pd.DataFrame(_PHYSICS_GRID, columns=["mu", "xsi", "tau0"])
    return rows.merge(grid, how="cross")


# EVENT helpers

def id_anriss_exists(id_anriss: int) -> bool:
    """Whether id_anriss appears in the event manifest -- a point lookup in
    the id_anriss-sorted event catalog on S3."""
    return _catalog_sort_key(id_anriss) is not None


# ── PIXEL — everything for one data-lake pixel (IFK view) ────────────────────

def get_pixel_gold_data(x: float, y: float) -> pd.DataFrame:
    """S3: raw gold-lake rows for one pixel — every contributing (anriss,
    parameter-combination) simulation that reached this exact (x, y). No
    joins, no derived columns. The kachel ID is a direct computation, so
    this is always exactly one S3 file read. Raises GoldNotReadyError if
    that kachel isn't finalized yet."""
    id_kachel = int(x // 1000) * 10000 + int(y // 1000)
    if id_kachel not in _finalized_kacheln():
        raise GoldNotReadyError([id_kachel])
    file = f"{GOLD_S3_ROOT}/id_kachel={id_kachel}/data.parquet"
    con = _s3_gold_connection()
    return con.execute("SELECT * FROM read_parquet(?) WHERE x = ? AND y = ?", [file, x, y]).df()


# ── AREA — raw rows over a bounding box (raster maps) ─────────────────────────

def get_area_gold_data(bbox: tuple[float, float, float, float]) -> pd.DataFrame:
    """S3: raw gold-lake rows for every kachel touching an LV95 bbox
    (xmin, ymin, xmax, ymax), filtered to the bbox. Every matching pixel's
    full raw data, nothing aggregated — one row per (pixel, anriss,
    parameter-combination). Raises GoldNotReadyError (naming the pending
    kacheln) if any kachel touching the bbox isn't finalized yet —
    consistent with PIXEL/EVENT, rather than silently under-representing
    the area."""
    xmin, ymin, xmax, ymax = bbox
    candidates = kacheln_in_bbox(*bbox)
    finalized = _finalized_kacheln()
    pending = [k for k in candidates if k not in finalized]
    if pending:
        raise GoldNotReadyError(pending)

    con = _s3_gold_connection()
    chunks = []
    for id_kachel in candidates:
        file = f"{GOLD_S3_ROOT}/id_kachel={id_kachel}/data.parquet"
        df = con.execute(
            "SELECT * FROM read_parquet(?) WHERE x >= ? AND x <= ? AND y >= ? AND y <= ?",
            [file, xmin, xmax, ymin, ymax],
        ).df()
        if not df.empty:
            chunks.append(df)
    return pd.concat(chunks, ignore_index=True) if chunks else _empty_gold_df()


# ── CATALOG — events index, counts (LOCAL, manifest-backed, shared by all views)

@lru_cache(maxsize=1)
def _catalog_con() -> duckdb.DuckDBPyConnection:
    """One S3 connection for event-catalog lookups, shared by all threads
    through _catalog_cursor(). Kept open for the process (unlike
    _s3_gold_connection) so DuckDB's parquet metadata cache keeps the
    catalog's footer: after the first lookup, one costs a single row-group
    read."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET memory_limit='{safe_duckdb_memory_limit()}'")
    con.execute("SET parquet_metadata_cache=true")
    configure_s3_for_duckdb(con)
    return con


_catalog_tls = threading.local()


def _catalog_cursor() -> duckdb.DuckDBPyConnection:
    """This thread's cursor on _catalog_con. DuckDB's SET s3_*/http_proxy are
    per connection and a cursor is a connection of its own, so every cursor
    gets the S3 settings again (without them it reads anonymously: 403)."""
    con = getattr(_catalog_tls, "con", None)
    if con is None:
        con = _catalog_con().cursor()
        configure_s3_for_duckdb(con)
        _catalog_tls.con = con
    return con


_CATALOG_COLUMNS = ["id_anriss", "id_prozessquelle", "x_anriss", "y_anriss", "d", "A", "h", "h_type", "sort_key"]


@lru_cache(maxsize=4096)
def _catalog_rows(id_anriss: int) -> pd.DataFrame:
    """The event catalog's rows for one anriss (one per h variant), read once
    per process: opening an anriss needs its parameters, its existence and the
    sort_key behind two fragment keys, and each S3 lookup costs ~0.1-0.4 s.
    The catalog is immutable, so caching can't go stale. Don't mutate the
    returned frame (callers copy)."""
    return _catalog_cursor().execute(
        f"""SELECT {', '.join(f'"{c}"' for c in _CATALOG_COLUMNS)}
            FROM read_parquet('{EVENT_CATALOG_S3_URI}') WHERE id_anriss = ?""",
        [int(id_anriss)],
    ).df()


def _catalog_sort_key(id_anriss: int) -> int | None:
    """id_anriss -> its manifest sort_key (identical for its h variants:
    both run in the same batch), None if it isn't in the manifest."""
    rows = _catalog_rows(int(id_anriss))
    return int(rows["sort_key"].iloc[0]) if len(rows) else None


@lru_cache(maxsize=1)
def _catalog_stats() -> dict:
    """The precomputed catalog totals (EVENT_CATALOG_STATS_KEY), read once
    per process. home_kacheln maps each anriss home kachel (the km2 tile of
    x_anriss/y_anriss) to {n_anriss, n_rows, areas, prozessquellen}."""
    body = get_s3_client().get_object(Bucket=S3_BUCKET_GOLD, Key=EVENT_CATALOG_STATS_KEY)["Body"].read()
    stats = json.loads(body)
    stats["home_kacheln"] = {int(k): v for k, v in stats["home_kacheln"].items()}
    return stats


def catalog_overview_stats() -> dict:
    """Summary of the full event manifest for the overview page: distinct
    anriss/area/prozessquelle counts and a map-centre point (WGS84, the
    transformed median x/y -- a cosmetic initial view). Precomputed in the
    catalog stats, no scan."""
    st = _catalog_stats()
    center_lon, center_lat = Transformer.from_crs("EPSG:2056", "EPSG:4326", always_xy=True).transform(
        st["center_x"], st["center_y"])
    return {
        "anriss_count": st["n_anriss"], "area_count": st["n_areas"], "pq_count": st["n_prozessquellen"],
        "center_lon": center_lon, "center_lat": center_lat,
    }


def gold_coverage_stats() -> dict:
    """LOCAL: how many anrisse have their OWN home kachel (the km² tile
    containing x_anriss/y_anriss) finalized — an approximate progress
    indicator, not a completeness guarantee: an anriss only avoids
    GoldNotReadyError once its whole reachable 5x5-kachel neighborhood is
    finalized (see _event_relevant_kacheln), and a finalized home kachel
    doesn't imply that. Cheap: the catalog stats' per-home-kachel counts
    (additive -- every anriss has exactly one home kachel) and value sets,
    restricted to the finalized kacheln. Replaced the old gold_anriss_kacheln index (2026-08-03):
    id_kachel is a deterministic function of x/y, so no index was ever
    needed, just this join."""
    finalized = _finalized_kacheln()
    covered = [v for k, v in _catalog_stats()["home_kacheln"].items() if k in finalized]
    if not covered:
        return {"anriss_count": 0, "area_count": 0, "pq_count": 0, "sim_count": 0}
    return {
        "anriss_count": sum(v["n_anriss"] for v in covered),
        "area_count": len({a for v in covered for a in v["areas"]}),
        "pq_count": len({q for v in covered for q in v["prozessquellen"]}),
        "sim_count": sum(v["n_rows"] for v in covered) * len(_PHYSICS_GRID),
    }


@lru_cache(maxsize=1)
def _expected_gold_kacheln() -> frozenset[int]:
    """Every kachel gold's finalize/compact phase can EVER close — the union
    of every anriss's home tile (the km² tile containing x_anriss/y_anriss)
    with that tile's full kachel_neighbors(ring=FINALIZE_RING) 5x5
    neighborhood (the same conservative fixed-ring set compact's closability
    check uses, see FINALIZE_RING/kachel_neighbors above and CLAUDE.md
    "Silver→Gold": "compact runs only on closable kacheln (all required
    batches scattered, 5x5 @ GOLD_MAX_REACH_M)"). Structurally an upper
    bound on _finalized_kacheln(): compact cannot close anything outside
    this set, so every currently-finalized kachel already lies inside it —
    including "empty" neighbor tiles that hold no anriss of their own and
    receive zero or few real pixels, closed anyway once their own 5x5
    requirement is satisfied.

    2026-08-06 bugfix: this reconciles progress.skerlak.ch and MAXI
    Explorer's "N finalized / total" numbers, which disagreed (413 vs 264
    over a shared total of 6,033) because neither side's total was this set.
    6,033 (monitoring/probe_progress_service.py's old len(TILES)) is a
    batch-visualization artifact (build_maxi_progress_page.py picks one
    *majority* tile per 100-event batch, so sparse/edge tiles that never won
    a batch are missing from it — verified 6,204 true home tiles vs 6,033
    batch-majority tiles against the live manifest). The plain home-tile
    count (6,204, this function's first pass before neighbors were added)
    is closer but still wrong the other way: it excludes real, legitimately
    finalized neighbor-only tiles from the numerator, undercounting
    completed work. This function (verified 7,960 against the live
    manifest) is the actual completion universe on both counts. Cached:
    derived purely from the static event manifest, never changes during a
    running process."""
    expected = set()
    for h in _catalog_stats()["home_kacheln"]:
        expected.update(kachel_neighbors(h))
    return frozenset(expected)


def expected_gold_kachel_count() -> int:
    """The true total kachel count gold's finalize phase will ever close
    (see _expected_gold_kacheln) — the denominator for the "Fertiggestellte
    Gold-Kacheln" progress metric."""
    return len(_expected_gold_kacheln())


def gold_finalized_kachel_count() -> int:
    """How many kacheln are finalized in gold, scoped to the true completion
    universe (_expected_gold_kacheln) — the numerator to pair with
    expected_gold_kachel_count(). The intersection is a defensive no-op in
    practice (see _expected_gold_kacheln's docstring: compact structurally
    can't finalize anything outside this set) but keeps the invariant
    explicit rather than assumed."""
    return len(_finalized_kacheln() & _expected_gold_kacheln())


def load_kachel_coverage() -> gpd.GeoDataFrame:
    """1-km squares of the currently finalized gold kacheln (LV95), per the
    LOCAL manifest — call refresh_gold_manifest() first to pick up newly
    finalized kacheln. The 'region' column labels each square (app tooltip).
    This is "show the finalized gold tiles" for the map: a kachel only
    appears here once gold has sealed it, never a partially-scattered one."""
    kacheln = sorted(_finalized_kacheln())
    geoms = [box((k // 10000) * 1000, (k % 10000) * 1000,
                 (k // 10000) * 1000 + 1000, (k % 10000) * 1000 + 1000)
             for k in kacheln]
    return gpd.GeoDataFrame({"region": [f"Kachel {k}" for k in kacheln]},
                            geometry=geoms, crs=CRS_LV95)


def count_simulations() -> int:
    """Total simulations the campaign produces -- every manifest row
    (id_anriss x h_type) x the full physics grid, from the catalog stats."""
    return _catalog_stats()["n_manifest_rows"] * len(_PHYSICS_GRID)
