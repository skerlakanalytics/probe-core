"""Standalone batch precompute of raster derivate over the S3 gold lake --
the same physics as probe_explorer's interactive Raster-Karten view
(derivate/maxi_ifk_and_raster.py's _run_mode_a/_run_mode_b, there called
"Modus A"/"Modus B" per the live UI's own German labels), decoupled from it:
this writes a persistent, config-driven MATRIX of pixel values instead of
computing one ad-hoc request on demand. Named here after what each half
actually COMPUTES rather than "A"/"B" (2026-08-25 rename -- the interactive
tool's own Mode A/B naming is left untouched, since it mirrors the deployed
app's actual UI labels and renaming that is a separate decision):

    return period (interactive tool's "Modus A", Wiederkehrperiode bei
    Schwellenwert): fix (variable, threshold) -> per-pixel
    return_period = 1 / P(intensity >= threshold).

    intensity (interactive tool's "Modus B", Intensität bei
    Wiederkehrperiode): fix return_period -> per-pixel intensity (all 3
    variables) at exceedance probability >= 1/T.

Per-kachel independence (why this can be a plain ActorPool, no scatter+
compact): unlike gold compact, which needs a 5x5 kachel neighborhood because
a pixel can land up to ~932m from its anriss (GOLD_MAX_REACH_M), a value
here only depends on rows already inside ITS OWN finalized SIM_SPATIAL
kachel file -- reach is fully resolved by the time a kachel is FINAL. So one
kachel is a complete, independent unit of work: read it, join the small
probability_lookup + ablauf tables (same join every interactive call does,
see _bind_gold_tile/_run_mode_a/_run_mode_b), compute, write. That also makes
this naturally incremental over a still-filling campaign (2026-08-25: ~45%
of the canton finalized) -- the pending set is just "finalized minus already
computed", recomputed fresh every pass; no separate "expected canton grid"
bookkeeping the way data_interface._expected_gold_kacheln needs for gold
compact's completion percentage.

Combined single pass (2026-08-25 decision, see architecture discussion): a
batch job pays each kachel's read+join cost ONCE and should amortize it
across the WHOLE precompute matrix, unlike the interactive tool which pays it
once per ad-hoc request. Both halves derive from the same per-variable
cumulative exceedance-probability table (p = P(intensity >= i) at each
distinct intensity level i actually present in that kachel) -- the
return-period side picks the exceedance probability AT a threshold (the
qualifying level closest to the threshold has the largest p, since p is
monotonically non-increasing in i); the intensity side picks the largest
intensity level whose p still clears 1/return_period. Building that
cumulative table (a windowed SUM, the expensive part) happens ONCE per
variable regardless of how many thresholds/return-periods are configured --
every extra combo after that is just another cheap FILTER aggregate in the
same GROUP BY x, y pass. See _combined_sql.

Output: one row per pixel, one column per configured combo (wide, not long --
column count is small and fixed per config). Written in THREE forms, every
kachel, by default -- a parquet with every combo as one column (return-period
and intensity columns together: parquet has no single-dtype-per-file
constraint, so there's no reason to split it), plus TWO multi-band GeoTIFFs,
one per kind (2026-08-25 decision: GDAL's GTiff driver has one dtype for the
WHOLE file, not per-band, and return-period values can be astronomically
large -- e.g. ~9.5e10 at an extreme threshold -- so mixing them with
intensity values in one file would force both onto a shared representation;
kept float32 for both, the split is for clean semantics, not a dtype
difference). Each GeoTIFF's bands are its kind's combos
(return_period_columns/intensity_columns order, band descriptions set to the
column name):
    Data-Lake-Derivate/raster/cfg_<hash>/parquet/id_kachel=<id>/data.parquet
    Data-Lake-Derivate/raster/cfg_<hash>/geotiff/return_period/kachel_<id>.tif
    Data-Lake-Derivate/raster/cfg_<hash>/geotiff/intensity/kachel_<id>.tif
A kachel with an empty half (e.g. an intensity-only precompute matrix) simply
gets no file for that kind. The <hash> is a short digest of the resolved
thresholds/return_periods matrix (config_hash below) -- NOT a ledger: output
existence under the LAST-written geotiff (intensity's if configured, else
return_period's -- see list_done_kacheln) IS the done-marker for that exact
config, same reasoning build_silver_to_gold_anriss.py/build_hull_fragments.py
already use at this lake's size (thousands of files, not billions -- a full
S3 listing under one prefix is cheap every pass). Editing config/jobs.yaml's
raster_derivate matrix therefore targets a NEW prefix; it never rewrites an
already-computed one in place.

Immutability/staleness (2026-08-25 decision): like SIM_SPATIAL, an already-
computed kachel is treated as immutable and is NEVER auto-recomputed. Unlike
SIM_SPATIAL, though, this job's OWN inputs beyond gold -- probability_lookup
and ablauf (Data-Lake-Probabilities-Rates/) -- are NOT immutable (Xurce
corrections do happen, see docs/claude-memory/project_maxi_delivery_qa.md).
This job does not detect or react to such a correction on its own; a
correction requires a deliberate manual rerun with --force (optionally scoped
to the affected kacheln via --kachel) to overwrite the stale rows for the
SAME config hash. That's a conscious simplicity tradeoff for v1, not an
oversight -- see the architecture discussion this module was built from.

Uploads are opt-in (--upload), like everything else that writes to the
bucket (see data_lake_schema.py's Derivate-artifacts note) -- without it,
output lands only under --out-dir locally, useful for a smoke test before
committing to an S3 write.

No expert-mode overrides here (p_h_mean/p_h_max/ablauf_override/anriss_
overrides) -- this batch job only ever computes the production-default
probabilities, matching SIM's own "physics never changes" philosophy.
Experten-Modus stays exclusively on the interactive path (build_raster_
for_bbox), which is unaffected by any of this.

Deploy: manual/on-demand for v1 (2026-08-25 decision) -- no loop/deploy
script yet. Run by hand (optionally --loop SECONDS for convenience) to catch
derivate up to the latest finalized kacheln; promote to a standing loop +
dedicated node later if this becomes a permanent job, same path silver_to_
gold_anriss/hull-fragments already took.

Usage:
    python derivate/build_raster_derivate.py                  # local-only smoke test
    python derivate/build_raster_derivate.py --upload          # + S3
    python derivate/build_raster_derivate.py --upload --limit 5      # smoke test: first 5 pending kacheln
    python derivate/build_raster_derivate.py --upload --force --kachel 2611017,2611018  # targeted manual rebuild

Worker resources (workers/cpus-per-worker/mem-per-worker-gb/duckdb-memory)
and the thresholds/return_periods matrix default from config/jobs.yaml
[raster_derivate] (CLI flags override the resource settings; the matrix
itself is config-only for now, see that section's header comment).

Running this batch job is a pipeline task: run it from ProBE_control_center,
which has config/jobs.yaml. pgr-atlas only imports this module's functions;
its config/app.yaml has no raster_derivate section, so the CLI here stops with
an error instead of computing with an empty matrix.
"""

import argparse
import hashlib
import json
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import ray
from ray.util.actor_pool import ActorPool

_REPO_ROOT = str(Path(__file__).resolve().parent.parent)
sys.path.insert(0, _REPO_ROOT)
# Also exported via PYTHONPATH (not just sys.path), and done before ray.init()
# below -- sys.path.insert only fixes imports in THIS (driver) process. Ray's
# ActorPool workers are separate subprocesses that inherit os.environ (so
# PYTHONPATH) but NOT the driver's runtime sys.path mutations. Unlike this
# script's siblings (build_silver_to_gold_anriss.py, build_hull_fragments.py),
# which are only ever launched via a loop script that `cd`s to the repo root
# first (see run_silver_to_gold_anriss_loop.sh/run_hull_fragments_loop.sh),
# this one is meant for direct manual invocation from wherever -- confirmed
# 2026-08-25: running `cd derivate && python build_raster_derivate.py` failed
# every worker with "ModuleNotFoundError: No module named 'utils'" without this.
os.environ['PYTHONPATH'] = os.pathsep.join(
    p for p in [_REPO_ROOT, os.environ.get('PYTHONPATH', '')] if p)
from probe_config import load_config  # noqa: E402
from utils import get_s3_resource, configure_s3_for_duckdb  # noqa: E402
from data_lake.data_lake_schema import DATA_LAKE_DIR_DERIVATE_RASTER  # noqa: E402
from data_lake.data_interface import GOLD_S3_ROOT, S3_BUCKET_GOLD  # noqa: E402
from derivate.maxi_ifk_and_raster import (  # noqa: E402
    INTENSITY_VARS, _GOLD_COL, _TO_DISPLAY, _lambda_ereignis_sql,
    _register_ablauf, _bind_probability_lookup, _assert_valid_lambda_ereignis,
    _is_missing_kachel, kachel_s3_file, _write_multiband,
)

_KINDS = ('return_period', 'intensity')


# ── Column naming + config hashing ────────────────────────────────────────────

# Identifier-safe unit suffix per variable -- INTENSITY_UNITS (maxi_ifk_and_
# raster.py) has display-only strings like 'm/s'/'kN/m²' that aren't valid in
# a column/band name.
_VAR_UNIT_SLUG = {'depth': 'm', 'velocity': 'ms', 'pressure': 'kpa'}


def _fmt_num(x) -> str:
    """Numeric value -> a SQL-identifier-safe, human-readable fragment --
    1.0 -> '1', 100.0 -> '100', 0.25 -> '0_25', 1000000 -> '1000000'. Whole
    numbers are never allowed through Python's ':g' formatting as-is:
    f'{1000000.0:g}' is '1e+06' (scientific notation kicks in past 6
    significant digits) -- '+' isn't even a valid identifier character, and
    it's the opposite of readable, which is the whole point of these names
    (2026-08-25: caught by a 1,000,000-year return period)."""
    xf = float(x)
    s = str(int(xf)) if xf == int(xf) else f"{xf:g}"
    return s.replace('.', '_').replace('-', 'neg')


def return_period_column(variable: str, threshold) -> str:
    """e.g. return_period_column('depth', 1.0) -> 'depth_1m_rp' -- the
    threshold (with its unit) upfront, '_rp' marking the column as a
    return-period OUTPUT (years) at that fixed threshold."""
    return f"{variable}_{_fmt_num(threshold)}{_VAR_UNIT_SLUG[variable]}_rp"


def intensity_column(variable: str, return_period) -> str:
    """e.g. intensity_column('depth', 10000) -> 'depth_rp10000y' -- the
    return period (years, 'y') upfront, output is an INTENSITY in the
    variable's own unit (see _VAR_UNIT_SLUG / INTENSITY_UNITS)."""
    return f"{variable}_rp{_fmt_num(return_period)}y"


def config_hash(thresholds: list[dict], return_periods: list[dict]) -> str:
    """Short, stable digest of the resolved precompute matrix -- see module
    docstring for why this is the versioning mechanism (a new hash = a new
    output prefix, not an in-place rewrite)."""
    payload = {
        'thresholds': sorted(({'variable': e['variable'], 'threshold': float(e['threshold'])}
                               for e in thresholds),
                              key=lambda e: (e['variable'], e['threshold'])),
        'return_periods': sorted(({'return_period': float(e['return_period'])} for e in return_periods),
                                  key=lambda e: e['return_period']),
    }
    return hashlib.sha1(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:8]


def config_prefix(cfg_hash: str) -> str:
    return f"{DATA_LAKE_DIR_DERIVATE_RASTER}/cfg_{cfg_hash}"


def parquet_prefix(cfg_hash: str) -> str:
    return f"{config_prefix(cfg_hash)}/parquet"


def geotiff_prefix(cfg_hash: str, kind: str) -> str:
    """kind: 'return_period' or 'intensity' -- written as separate GeoTIFFs
    (2026-08-25 decision): GDAL's GTiff driver has one dtype for the WHOLE
    file, not per-band, so mixing return-period values (can be
    astronomically large, e.g. ~9.5e10 for a rare/extreme threshold) and
    intensity values (m/m/s/kPa) into one file forces both onto a single
    shared representation. Kept float32 for both (simplest, and at the
    return-period side's observed magnitudes exact-year integer precision is
    meaningless anyway) -- the split is for clean semantics, not a
    dtype-per-file need."""
    assert kind in _KINDS
    return f"{config_prefix(cfg_hash)}/geotiff/{kind}"


def geotiff_filename(id_kachel: int, kind: str) -> str:
    """kind in the filename itself, not just the directory (2026-08-25 fix):
    downloading one file from geotiff/return_period/ and one from
    geotiff/intensity/ into the SAME local folder (e.g. dragging both into
    one Downloads dir before opening in QGIS) silently collided -- both were
    named identically (kachel_<id>.tif), so the second download clobbered or
    corrupted the first depending on how the download handled the conflict.
    Now: kachel_<id>_<kind>.tif."""
    assert kind in _KINDS
    return f"kachel_{id_kachel}_{kind}.tif"


# ── Combined-pass SQL ──────────────────────────────────────────────────────────

def _combined_sql(thresholds: list[dict], return_periods: list[dict]) -> str:
    """Single query over `base` (x, y, Fliesstiefe, Fliessgeschwindigkeit,
    Druck, lambda_ereignis -- see compute_kachel_derivate) computing every
    configured return-period/intensity column for all three variables. See
    module docstring for the shared-cumulative-table argument this relies on."""
    rps = [e['return_period'] for e in return_periods]
    branches, cols_by_var = [], {}
    for var in INTENSITY_VARS:
        col, f = _GOLD_COL[var], _TO_DISPLAY[var]
        var_thresholds = [e['threshold'] for e in thresholds if e['variable'] == var]
        exprs = []
        for t in var_thresholds:
            raw_t = t / f
            name = return_period_column(var, t)
            exprs.append(
                f'CASE WHEN MAX(p) FILTER (WHERE i >= {raw_t!r}) > 0 '
                f'THEN 1.0 / MAX(p) FILTER (WHERE i >= {raw_t!r}) END AS "{name}"'
            )
        for rp in rps:
            name = intensity_column(var, rp)
            exprs.append(f'MAX(i) FILTER (WHERE p >= {1.0 / rp!r}) * {f!r} AS "{name}"')

        names = [return_period_column(var, t) for t in var_thresholds] + \
                 [intensity_column(var, rp) for rp in rps]
        cols_by_var[var] = names
        select_extra = (',\n                   ' + ',\n                   '.join(exprs)) if exprs else ''
        branches.append(f"""
    {var}_agg AS (SELECT x, y, "{col}" AS i, SUM(lambda_ereignis) AS s
                  FROM base GROUP BY x, y, "{col}"),
    {var}_exc AS (SELECT x, y, i,
                         SUM(s) OVER (PARTITION BY x, y ORDER BY i DESC
                                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS p
                  FROM {var}_agg),
    {var}_res AS (SELECT x, y{select_extra}
                  FROM {var}_exc GROUP BY x, y)""")

    select_cols = ["COALESCE(depth_res.x, velocity_res.x, pressure_res.x) AS x",
                   "COALESCE(depth_res.y, velocity_res.y, pressure_res.y) AS y"]
    for var in INTENSITY_VARS:
        select_cols.extend(f'{var}_res."{name}"' for name in cols_by_var[var])

    return (f"WITH{','.join(branches)}\n"
            f"SELECT {', '.join(select_cols)}\n"
            f"FROM depth_res FULL JOIN velocity_res USING (x, y) FULL JOIN pressure_res USING (x, y)")


def return_period_columns(thresholds: list[dict]) -> list[str]:
    """All return-period output column names, grouped by variable
    (INTENSITY_VARS order) -- the GeoTIFF band list for the return_period
    file (see RasterDerivateWorker.build_kachel). Independent of
    _combined_sql's own emission order: _write_multiband indexes df columns
    by NAME, not position, so this only needs to be a valid, complete
    column-name list."""
    cols = []
    for var in INTENSITY_VARS:
        cols += [return_period_column(var, e['threshold']) for e in thresholds if e['variable'] == var]
    return cols


def intensity_columns(return_periods: list[dict]) -> list[str]:
    """All intensity output column names, grouped by variable -- the GeoTIFF
    band list for the intensity file."""
    cols = []
    for var in INTENSITY_VARS:
        cols += [intensity_column(var, e['return_period']) for e in return_periods]
    return cols


def matrix_columns(thresholds: list[dict], return_periods: list[dict]) -> list[str]:
    """All output column names (excluding x, y) a given matrix produces --
    the parquet's full column set (parquet keeps both kinds together, unlike
    the GeoTIFF split -- parquet has no single-dtype-per-file constraint, so
    there's no reason to split it)."""
    return return_period_columns(thresholds) + intensity_columns(return_periods)


# ── Per-kachel compute ─────────────────────────────────────────────────────────

def compute_kachel_derivate(con: duckdb.DuckDBPyConnection, id_kachel: int,
                            thresholds: list[dict], return_periods: list[dict]) -> pd.DataFrame | None:
    """One finalized SIM_SPATIAL kachel -> its derivate DataFrame, or None if
    the kachel turns out missing (404 -- shouldn't happen for a kachel a
    caller already confirmed finalized, but the same defensive check
    maxi_ifk_and_raster._bind_gold_tile uses is cheap insurance against a
    listing that's gone stale mid-run) or empty. `con` must already have
    `ablauf` registered (see RasterDerivateWorker.__init__ -- shared across
    every kachel one actor processes, registered once, not per kachel)."""
    try:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE gold_tile AS
            SELECT * FROM read_parquet('{kachel_s3_file(id_kachel)}')
        """)
    except duckdb.HTTPException as e:
        if _is_missing_kachel(e):
            return None
        raise
    _bind_probability_lookup(con, "SELECT DISTINCT id_anriss FROM gold_tile")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE base AS
        SELECT dg.x, dg.y, dg."Fliesstiefe", dg."Fliessgeschwindigkeit", dg."Druck", pl.lambda_Hangmuren,
               {_lambda_ereignis_sql()} AS lambda_ereignis
        FROM gold_tile dg
        JOIN probability_lookup pl USING (id_anriss)
        JOIN ablauf ab ON dg.mu = ab.mu AND dg.xsi = ab.xsi AND dg.tau0 = ab.tau0
    """)
    _assert_valid_lambda_ereignis(con, "SELECT lambda_ereignis, lambda_Hangmuren FROM base", f"kachel {id_kachel} (raster derivate)")
    df = con.execute(_combined_sql(thresholds, return_periods)).df()
    return df if not df.empty else None


# ── Ray actor + pool driver (same operating model as build_silver_to_gold_anriss.py) ──

def worker_temp_dir(out_dir: str, pid: int) -> str:
    """Per-actor DuckDB temp_directory -- must be unique per actor (pid), not
    just per out_dir. A shared temp_directory across concurrent actors lets
    two independent DuckDB processes' spill files collide on the same
    internal filename (e.g. "duckdb_temp_storage_S32K-1.tmp" isn't
    pid/uuid-qualified), corrupting each other's spilled blocks -- caught
    2026-08-25 on the first real workers>1 run (every single-worker test
    before that had just one DuckDB process, so the collision was
    structurally impossible to hit)."""
    return str(Path(out_dir) / '.duckdb_tmp' / f'worker_{pid}')


def _connect(duckdb_memory: str, temp_dir: str) -> duckdb.DuckDBPyConnection:
    """One DuckDB connection per ray actor, threads=1 -- parallelism comes
    from the ray worker count, same fixed rule as data_lake_gold_worker.py/
    build_silver_to_gold_anriss.py's _connect."""
    con = duckdb.connect(':memory:')
    con.execute(f"SET max_memory='{duckdb_memory}'")
    os.makedirs(temp_dir, exist_ok=True)
    con.execute(f"SET temp_directory='{temp_dir}'")
    # Hard cap, defense-in-depth: found 2026-09-13 that _recycle_connection's
    # close-sweep-reopen (added 2026-08-28 for exactly this failure mode --
    # see that method's docstring) is NOT fully preventing the leak in
    # practice -- a real run left 17-22GB per worker despite it running
    # after every single kachel. Root cause not re-investigated (possibly a
    # DuckDB version behavior change since that fix; worth a fresh look
    # rather than trusting this comment's guess). This cap doesn't fix the
    # leak, it just bounds the blast radius: instead of silently filling
    # the whole disk (the 2026-08-28 incident, and again 2026-09-13), a
    # kachel needing more spill than this simply fails that one kachel
    # (retryable) rather than taking the node down.
    #
    # Deliberately NOT set equal to duckdb_memory (tried that first, wrong):
    # spilling to disk exists specifically to handle work that EXCEEDS the
    # in-memory budget, so a disk cap equal to the memory cap leaves no real
    # spill room at all -- caused every worker to OOM immediately on real
    # kacheln that legitimately need to spill. 10x duckdb_memory as a rough
    # "generous but still bounded" rule of thumb; cheap to be generous given
    # this only matters on nodes with real disk headroom in the first place.
    max_temp_gb = float(duckdb_memory.rstrip("GBgb")) * 10
    con.execute(f"SET max_temp_directory_size='{max_temp_gb}GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=1")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    configure_s3_for_duckdb(con)
    return con


class RasterDerivateWorker:
    """Ray actor: computes + writes one finalized kachel's raster derivate at
    a time. Kacheln never shuffle across each other (see module docstring),
    so one actor can own a kachel end-to-end with no coordination needed."""

    def __init__(self, thresholds, return_periods, out_dir, cfg_hash, duckdb_memory, upload, bucket):
        self.thresholds, self.return_periods = thresholds, return_periods
        self.out_dir = Path(out_dir)
        self.cfg_hash = cfg_hash
        self.upload = upload
        self.bucket = bucket
        self._duckdb_memory = duckdb_memory   # kept for reopening the connection
                                               # between kacheln -- see build_kachel
        self.temp_dir = Path(worker_temp_dir(out_dir, os.getpid()))
        self.con = _connect(duckdb_memory, str(self.temp_dir))
        _register_ablauf(self.con)
        self.s3_resource = get_s3_resource() if upload else None

    def build_kachel(self, id_kachel: int):
        # 2026-08-28: the connection recycle below (see the comment further
        # down for why it exists and why it must run this way) needs to
        # happen regardless of which return path this takes -- compute_
        # kachel_derivate() runs a real query, and can leave temp/spill
        # files behind, even on the df-is-None early-return path (an empty
        # kachel still ran the query, it just had nothing to write). A
        # try/finally guarantees that, unlike the first version of this fix
        # which only ran on the normal path and left this same gap open.
        try:
            return self._build_kachel(id_kachel)
        finally:
            self._recycle_connection()

    def _build_kachel(self, id_kachel: int):
        df = compute_kachel_derivate(self.con, id_kachel, self.thresholds, self.return_periods)
        if df is None:
            return id_kachel, 0

        rp_cols = return_period_columns(self.thresholds)
        intensity_cols = intensity_columns(self.return_periods)
        base_tags = {'id_kachel': str(id_kachel), 'config_hash': self.cfg_hash}
        parquet_key = f"{parquet_prefix(self.cfg_hash)}/id_kachel={id_kachel}/data.parquet"
        # rp_key/intensity_key are None when that half's list is empty in
        # config/jobs.yaml (e.g. an intensity-only matrix) -- no file is
        # written for an empty kind.
        rp_key = f"{geotiff_prefix(self.cfg_hash, 'return_period')}/{geotiff_filename(id_kachel, 'return_period')}" if rp_cols else None
        intensity_key = f"{geotiff_prefix(self.cfg_hash, 'intensity')}/{geotiff_filename(id_kachel, 'intensity')}" if intensity_cols else None

        if self.upload:
            tmp_dir = self.out_dir / '.upload_tmp'
            tmp_dir.mkdir(parents=True, exist_ok=True)
            parquet_tmp = tmp_dir / f"kachel_{id_kachel}.parquet"
            df.to_parquet(parquet_tmp, compression='zstd', index=False)
            self.s3_resource.Bucket(self.bucket).upload_file(str(parquet_tmp), parquet_key)
            parquet_tmp.unlink()
            if rp_cols:
                rp_tmp = tmp_dir / f"kachel_{id_kachel}_rp.tif"
                _write_multiband(df, rp_cols, str(rp_tmp), {**base_tags, 'kind': 'return_period'})
                self.s3_resource.Bucket(self.bucket).upload_file(str(rp_tmp), rp_key)
                rp_tmp.unlink()
            if intensity_cols:
                intensity_tmp = tmp_dir / f"kachel_{id_kachel}_intensity.tif"
                _write_multiband(df, intensity_cols, str(intensity_tmp), {**base_tags, 'kind': 'intensity'})
                self.s3_resource.Bucket(self.bucket).upload_file(str(intensity_tmp), intensity_key)
                intensity_tmp.unlink()
        else:
            parquet_local = self.out_dir / parquet_key
            parquet_local.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(parquet_local, compression='zstd', index=False)
            if rp_cols:
                rp_local = self.out_dir / rp_key
                rp_local.parent.mkdir(parents=True, exist_ok=True)
                _write_multiband(df, rp_cols, str(rp_local), {**base_tags, 'kind': 'return_period'})
            if intensity_cols:
                intensity_local = self.out_dir / intensity_key
                intensity_local.parent.mkdir(parents=True, exist_ok=True)
                _write_multiband(df, intensity_cols, str(intensity_local), {**base_tags, 'kind': 'intensity'})

        return id_kachel, len(df)

    def _recycle_connection(self):
        """2026-08-28: real disk-full production incident traced to THIS
        connection's own DuckDB spill files never being cleaned up -- not
        just across restarts (see main()'s pre-run sweep), but WITHIN one
        long-lived actor's own lifetime too: a single ~35min window already
        left 22-38GB per worker (65 orphaned worker dirs total, 620GB,
        filled a 985GB disk to 0 free).

        First attempt at a fix (same day) swept self.temp_dir's contents
        directly while self.con stayed open across the whole actor's
        lifetime -- wrong: DuckDB's buffer manager holds references to its
        own temp/spill files for the CONNECTION's lifetime, not just the
        query that created them, so deleting a file out from under a
        still-open connection is unsafe even between calls, not just
        mid-query. Caused real job failures on the very next pass this was
        deployed: `_duckdb.IOException: Could not remove file ".../
        duckdb_temp_storage_S192K-0.tmp": No such file or directory` --
        DuckDB itself trying to manage a file this code had already
        deleted out from under it.

        Fixed properly: close the connection first (DuckDB cleans up its
        own temp files as part of normal shutdown, the way it's actually
        designed to), THEN sweep anything left over (now safe, nothing has
        it open), THEN reopen a fresh connection on the same temp_dir for
        the next kachel. Called from build_kachel's `finally`, so it runs
        on every exit path including the df-is-None early return (that
        path still ran a real query first, and can still leave spill files
        behind). Reopening costs a few hundred ms at most (in-memory
        DuckDB + httpfs/S3 config + re-registering the ablauf UDF) against
        per-kachel compute times of tens of seconds to minutes -- a real
        but small cost for correctness."""
        self.con.close()
        for f in self.temp_dir.glob('*'):
            if f.is_dir():
                shutil.rmtree(f, ignore_errors=True)
            else:
                f.unlink(missing_ok=True)
        self.con = _connect(self._duckdb_memory, str(self.temp_dir))
        _register_ablauf(self.con)


def _run_pool(worker_cls, actor_kwargs, method_name, jobs, n_workers, cpus_per_worker, mem_per_worker_gb,
              progress_every=None):
    """Run jobs on an ActorPool of worker_cls actors -- same operating model
    as build_silver_to_gold_anriss.py's _run_pool / data_lake_gold_worker.py's
    GoldRunCoordinator._run_pool (worker resources declared to ray as
    num_cpus/memory, __ray_ready__ fail-fast so a misconfigured worker count
    errors immediately instead of hanging the pool forever). progress_every:
    print a one-line "N/total done" every that many completions (None, the
    default, keeps the original silent-until-the-end-summary behavior --
    this job's own caller, run_pass(), already prints a start/end line, and
    a wall of per-kachel output isn't wanted for a many-thousand-kachel
    pass). 2026-09-04: added for build_affected_mask.py, whose per-kachel
    query is cheap enough that a full pass can otherwise sit quiet for a
    long stretch with nothing on stdout between the start line and the end
    summary."""
    if not jobs:
        return []
    if not ray.is_initialized():
        ray.init(configure_logging=False, object_store_memory=10**9)
    RemoteCls = ray.remote(
        num_cpus=cpus_per_worker,
        memory=int(mem_per_worker_gb * 1024**3),
        max_restarts=3,
    )(worker_cls)
    actors = [RemoteCls.remote(**actor_kwargs) for _ in range(min(n_workers, len(jobs)))]

    try:
        ray.get([a.__ray_ready__.remote() for a in actors], timeout=60)
    except ray.exceptions.GetTimeoutError:
        raise ValueError(
            f"Not all {len(actors)} workers ({cpus_per_worker} CPU / "
            f"{mem_per_worker_gb} GB each) could be scheduled within 60s — "
            f"the machine is too small for this configuration. Lower --workers, "
            f"--cpus-per-worker or --mem-per-worker-gb. "
            f"Available: {ray.available_resources()}")

    pool = ActorPool(actors)
    for job in jobs:
        pool.submit(lambda actor, j: getattr(actor, method_name).remote(*j), job)
    results = []
    n_failed = 0
    n_total = len(jobs)
    while pool.has_next():
        try:
            results.append(pool.get_next_unordered())
        except Exception as e:
            n_failed += 1
            print(f"❌ {worker_cls.__name__}.{method_name} job failed: {e}", flush=True)
        n_done = len(results) + n_failed
        if progress_every and (n_done % progress_every == 0 or n_done == n_total):
            print(f"  {n_done}/{n_total} done ({n_failed} failed)", flush=True)
    for actor in actors:
        ray.kill(actor)
    if n_failed:
        raise ValueError(f"{n_failed}/{len(jobs)} {method_name} jobs failed — rerun to retry them")
    return results


# ── Kachel-set bookkeeping (no ledger -- see module docstring) ───────────────

def _glob_kacheln(con: duckdb.DuckDBPyConnection, s3_prefix: str) -> set[int]:
    try:
        files = con.execute(f"SELECT file FROM glob('{s3_prefix}/id_kachel=*/data.parquet')").fetchall()
    except duckdb.IOException:
        return set()
    return {int(f[0].split('id_kachel=')[1].split('/')[0]) for f in files}


def list_finalized_kacheln(con: duckdb.DuckDBPyConnection) -> set[int]:
    """Direct S3 listing, not data_interface._finalized_kacheln()'s LOCAL
    manifest -- that manifest is probe_explorer app-local cached state, and
    this standalone pipeline shouldn't assume it exists or is fresh. Cheap
    at this lake's size (thousands of kachel dirs), same technique
    data_interface.refresh_gold_manifest uses internally."""
    return _glob_kacheln(con, GOLD_S3_ROOT)


def list_precomputed_kacheln(con: duckdb.DuckDBPyConnection, cfg_hash: str,
                             has_return_periods: bool, bucket: str = S3_BUCKET_GOLD) -> set[int]:
    """Read-only S3 check for "which kacheln are already precomputed for
    this cfg_hash" -- the same query list_done_kacheln's --upload branch
    needs for its own resumability check, factored out so a read-only
    consumer (e.g. probe_explorer's Derivate view) doesn't have to drag in
    the batch job's out_dir/upload/local-mode parameters it has no use for.
    marker_kind matches list_done_kacheln's own choice exactly: the
    intensity file is written last when both halves are configured (see
    RasterDerivateWorker.build_kachel), so its presence implies the
    return_period file -- and the parquet -- already succeeded too."""
    marker_kind = 'intensity' if has_return_periods else 'return_period'
    prefix = geotiff_prefix(cfg_hash, marker_kind)
    suffix = f'_{marker_kind}.tif'
    try:
        files = con.execute(f"SELECT file FROM glob('s3://{bucket}/{prefix}/kachel_*{suffix}')").fetchall()
    except duckdb.IOException:
        return set()
    return {int(f[0].rsplit('/', 1)[-1][len('kachel_'):-len(suffix)]) for f in files}


def list_config_manifests(bucket: str = S3_BUCKET_GOLD) -> list[dict]:
    """All uploaded raster-derivate config manifests (_config.json, one per
    cfg_<hash> prefix that's ever had a completed --upload pass) -- read-only
    discovery for a consumer that wants to know which precompute matrices
    exist on S3 without recomputing config_hash() from its own local
    config/jobs.yaml checkout (which could silently be out of sync with
    whatever actually produced the S3 data it's about to link to -- two
    separate repos read this module, see probe_explorer's Derivate view)."""
    s3 = get_s3_resource()
    manifests = []
    for obj in s3.Bucket(bucket).objects.filter(Prefix=f"{DATA_LAKE_DIR_DERIVATE_RASTER}/cfg_"):
        if obj.key.endswith('/_config.json'):
            manifests.append(json.loads(obj.get()['Body'].read()))
    return manifests


def latest_config_manifest(bucket: str = S3_BUCKET_GOLD) -> dict | None:
    """Most-recently-updated manifest, or None if none have been uploaded yet
    (e.g. before main()'s eager upload lands for a from-scratch run -- a full
    catch-up pass over the whole finalized set can take days, and the
    manifest needs to exist from the START of that, not just once it
    finishes, for any consumer relying on this)."""
    manifests = list_config_manifests(bucket)
    return max(manifests, key=lambda m: m['updated_at']) if manifests else None


def list_done_kacheln(con: duckdb.DuckDBPyConnection, out_dir: Path, cfg_hash: str,
                      bucket: str, upload: bool, thresholds: list[dict], return_periods: list[dict]) -> set[int]:
    """Kacheln already computed for this exact config hash -- checked against
    S3 when --upload is set (S3 is then the resumability source of truth
    across machines/runs, see list_precomputed_kacheln), otherwise against
    --out-dir locally. Checked via ONE geotiff, not the parquet --
    build_kachel writes the geotiff(s) last, so a geotiff's presence implies
    the parquet write already succeeded too (see module docstring)."""
    if upload:
        return list_precomputed_kacheln(con, cfg_hash, bool(return_periods), bucket)
    marker_kind = 'intensity' if return_periods else 'return_period'
    prefix = geotiff_prefix(cfg_hash, marker_kind)
    suffix = f'_{marker_kind}.tif'
    base = out_dir / prefix
    if not base.exists():
        return set()
    return {int(p.name[len('kachel_'):-len(suffix)]) for p in base.glob(f'kachel_*{suffix}')}


# ── CLI ────────────────────────────────────────────────────────────────────────

def run_pass(out_dir, cfg_hash, thresholds, return_periods, upload, bucket,
             n_workers, cpus_per_worker, mem_per_worker_gb, duckdb_memory,
             only_kacheln=None, force=False, limit=None):
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    configure_s3_for_duckdb(con)

    finalized = list_finalized_kacheln(con)
    if only_kacheln:
        finalized &= set(only_kacheln)
    done = set() if force else list_done_kacheln(
        con, out_dir, cfg_hash, bucket, upload, thresholds, return_periods)
    con.close()

    pending = sorted(finalized - done)
    if limit:
        pending = pending[:limit]
    print(f"[{datetime.now(timezone.utc).isoformat()}] cfg_{cfg_hash}: "
          f"{len(finalized)} finalized, {len(done)} already done, {len(pending)} pending "
          f"({n_workers} ray workers, {cpus_per_worker} CPU / {mem_per_worker_gb} GB each)", flush=True)

    results = _run_pool(
        RasterDerivateWorker,
        dict(thresholds=thresholds, return_periods=return_periods, out_dir=str(out_dir), cfg_hash=cfg_hash,
             duckdb_memory=duckdb_memory, upload=upload, bucket=bucket),
        "build_kachel", [(id_kachel,) for id_kachel in pending],
        n_workers, cpus_per_worker, mem_per_worker_gb)

    n_empty = sum(1 for _, n_rows in results if n_rows == 0)
    n_written = len(results) - n_empty
    print(f"  {n_written} kachel(s) written, {n_empty} empty (no pixels)", flush=True)

    if upload and results:
        _upload_config_manifest(bucket, cfg_hash, thresholds, return_periods)
    return len(pending)


def _upload_config_manifest(bucket, cfg_hash, thresholds, return_periods):
    """Small human-readable record of what cfg_<hash> means -- the hash alone
    isn't self-describing; this is purely for someone inspecting the bucket,
    never read back by the pipeline itself (config_hash is recomputed fresh
    from config/jobs.yaml every run)."""
    manifest = {
        'config_hash': cfg_hash, 'thresholds': thresholds, 'return_periods': return_periods,
        'columns': matrix_columns(thresholds, return_periods),
        'updated_at': datetime.now(timezone.utc).isoformat(),
    }
    s3 = get_s3_resource()
    s3.Object(bucket, f"{config_prefix(cfg_hash)}/_config.json").put(
        Body=json.dumps(manifest, indent=2).encode())


def parse_args():
    # Pipeline job settings; absent from pgr-atlas's config/app.yaml (see the
    # module docstring), so both fall back to empty and the check below stops.
    cfg = load_config()
    rd = cfg.get("raster_derivate", {})
    sg = cfg.get("silver_to_gold", {})
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bucket", default=sg.get("s3_bucket_gold", S3_BUCKET_GOLD),
                    help="S3 bucket holding Data-Lake-Gold/ (default: config/jobs.yaml silver_to_gold.s3_bucket_gold)")
    p.add_argument("--out-dir", default="~/probe_data/raster_derivate",
                    help="Local root for output (and the resumability source of truth when --upload is not set)")
    p.add_argument("--upload", action="store_true",
                    help="Also write to s3://<bucket>/Data-Lake-Derivate/raster/ (opt-in, off by default)")
    p.add_argument("--loop", type=int, metavar="SECONDS", default=None,
                    help="Repeat forever, sleeping this many seconds between passes (default: run one pass and exit)")
    p.add_argument("--limit", type=int, default=None,
                    help="Only process the first N pending kacheln this pass (smoke test)")
    p.add_argument("--kachel", default=None,
                    help="Comma-separated id_kachel list -- scope this run to specific kacheln "
                         "(e.g. a targeted manual rebuild after a probability_lookup/ablauf correction)")
    p.add_argument("--force", action="store_true",
                    help="Recompute even if already done for this config hash (manual rebuild after "
                         "a probability_lookup/ablauf correction -- see module docstring's Immutability section)")
    p.add_argument("--workers", type=int, default=rd.get("workers", 4),
                    help="Number of ray workers (default: config/jobs.yaml raster_derivate.workers)")
    p.add_argument("--cpus-per-worker", type=int, default=rd.get("cpus_per_worker", 1))
    p.add_argument("--mem-per-worker-gb", type=float, default=rd.get("mem_per_worker_gb", 4))
    p.add_argument("--duckdb-memory", default=rd.get("duckdb_memory", "3GB"),
                    help="DuckDB max_memory cap inside each worker")
    args = p.parse_args()
    args.out_dir = Path(args.out_dir).expanduser()
    args.only_kacheln = [int(k) for k in args.kachel.split(',')] if args.kachel else None
    args.thresholds = rd.get("thresholds", [])
    args.return_periods = rd.get("return_periods", [])
    if not args.thresholds and not args.return_periods:
        p.error("raster_derivate.thresholds and .return_periods are both empty — run this job from ProBE_control_center (config/jobs.yaml), nothing to compute")
    return args


def main():
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    # 2026-08-28: caught a real disk-full production incident -- each ray
    # actor's own worker_temp_dir() (keyed by pid, see that function) was
    # never cleaned up on exit, so every restart of this loop left its
    # workers' entire DuckDB spill history behind as dead weight under a
    # NEW pid. Traced back to worker directories dating to 2026-08-25 (the
    # very first multi-worker run) still present 3 days and several
    # restarts later -- 65 orphaned worker_<pid> dirs, 620GB, filled a
    # 985GB disk to 0 bytes free and crashed the whole ray cluster (Ray's
    # own event-log flush failing with "No space left on device").
    # Anything already in .duckdb_tmp/ at this point is guaranteed
    # orphaned -- THIS run hasn't created any worker actors (and their
    # pids) yet, so every existing worker_<pid> subdirectory belongs to a
    # previous, no-longer-running invocation. Clear it before workers
    # start, not after they finish -- a from-scratch catch-up pass can run
    # for many hours, so cleaning only at the end would still let one run's
    # own spill files accumulate unbounded across that whole pass.
    duckdb_tmp_root = args.out_dir / '.duckdb_tmp'
    if duckdb_tmp_root.exists():
        shutil.rmtree(duckdb_tmp_root, ignore_errors=True)

    cfg_hash = config_hash(args.thresholds, args.return_periods)

    if args.upload:
        # Eager, not just after run_pass finishes (see run_pass's own
        # end-of-pass upload) -- a from-scratch catch-up pass over the whole
        # finalized set can take days, and a consumer (e.g. probe_explorer's
        # Derivate view, via latest_config_manifest) needs the manifest to
        # exist from the START of a run, not just once it completes.
        _upload_config_manifest(args.bucket, cfg_hash, args.thresholds, args.return_periods)

    def one_pass():
        run_pass(args.out_dir, cfg_hash, args.thresholds, args.return_periods, args.upload, args.bucket,
                  args.workers, args.cpus_per_worker, args.mem_per_worker_gb, args.duckdb_memory,
                  only_kacheln=args.only_kacheln, force=args.force, limit=args.limit)

    if args.loop is None:
        one_pass()
        return
    while True:
        try:
            one_pass()
        except Exception as e:
            print(f"pass FAILED: {e}", flush=True)
        print(f"sleeping {args.loop}s", flush=True)
        time.sleep(args.loop)


if __name__ == "__main__":
    main()
