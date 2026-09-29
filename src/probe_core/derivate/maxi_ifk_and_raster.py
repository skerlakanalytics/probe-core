"""MAXI gold lake derivate: default-scenario IFK and rasters, per-km² kacheln.

Reads the S3-backed per-km² Hive gold lake (Data-Lake-Gold/SIM_SPATIAL/
id_kachel=NNNNNNNN/data.parquet, schema data_lake/data_lake_schema.py's
GOLD_SCHEMA_SIM_DUCKDB) — same S3 access data_lake/data_interface.py uses for
the map view (GOLD_S3_ROOT, _s3_gold_connection). The kacheln ARE the spatial index:
a pixel/bbox query opens only the touched kachel files, and row-group
pruning on the leading (x, y) sort handles the rest.

Only the default scenario is computed (decision Bojan 2026-07-16 for the MAXI explorer): all data, p_h computed inline from gold's own h/d
columns (h == d -> GOLD_P_H_MEAN, else GOLD_P_H_MAX — SIM gold carries no
p_h column), lambda_Hangmuren/p_raeumlich/p_A joined from the small
per-id_anriss probability lookup (derivate/build_probability_lookup.py,
Data-Lake-Probabilities-Rates/probability_lookup.parquet), p_Ablauf from the
geo7 table ablaufwahrscheinlichkeiten.csv (same folder, matches the
36-combination MAXI grid, sums to 1). p_A is NULL-free as of the 2026-08-11
Xurce redelivery (the 6
rows that had NULL p_A were patched at the source, see
docs/claude-memory/project_maxi_delivery_qa.md) -- _assert_valid_lambda_ereignis
below crashes loudly on a recurrence rather than silently dropping rows.

    lambda_Ereignis = lambda_Hangmuren · p_raeumlich · p_A · p_h · p_Ablauf

Decision 2026-08-13 (see docs/claude-memory/project_gold_kachel_design.md):
a live join against the small probability lookup + the tiny p_Ablauf table,
not a materialized ENRICHED gold lake. lambda_Hangmuren/p_raeumlich/p_A are
per-id_anriss scalars — baking them onto every pixel row of every simulation
(what a full ENRICHED lake would do) would duplicate a tiny table across
gold's billions of rows and force a full lake rewrite on every probability
correction; a small, cheaply-rebuildable lookup avoids both. The probability
lookup is reduced to just the touched id_anriss before joining (same
memory-conscious pattern as workers/data_lake_gold_worker.py's
GoldCompactWorker.compact_kachel — its kachel_lookup reduction is exactly
why the original full-lookup join OOM'd and got fixed this way).
"""

import hashlib
import json
import os
import shutil
import time
import uuid
from datetime import datetime
from functools import lru_cache

import duckdb
import numpy as np
import pandas as pd
import rasterio
from rasterio.crs import CRS
from rasterio.transform import from_origin

# Single source of truth for the pixel↔anriss reach bound (nominal DEM cap is
# 800 m, measured up to ~932 m — see data_lake_schema.py). Re-exported for the
# explorer's kachel-candidate logic.
from probe_core.data_lake.data_lake_schema import (  # noqa: F401 (GOLD_MAX_REACH_M is re-exported)
    GOLD_MAX_REACH_M, GOLD_P_H_MEAN, GOLD_P_H_MAX,
    DATA_LAKE_PROBABILITIES_RATES_LOOKUP, DATA_LAKE_PROBABILITIES_RATES_ABLAUF,
    DATA_LAKE_DIR_GOLD_SIM_SPATIAL,
)
from probe_core.data_lake.data_interface import (
    GOLD_S3_ROOT, S3_BUCKET_GOLD, _s3_gold_connection,
)
from probe_core.resources import (
    container_cpu_limit, container_memory_limit_bytes,
    duckdb_max_temp_directory_size, safe_duckdb_memory_limit,
)
from probe_core.s3 import configure_s3_for_duckdb, get_s3_client

# p_Ablauf (geo7 table) -- S3 mirror of the git-tracked input/ file, read the
# same way as the probability lookup (no local-file dependency; every caller
# already has S3 configured on its connection before _register_ablauf runs,
# see _s3_gold_connection / build_raster_for_bbox).
PROBABILITY_LOOKUP_S3 = f"s3://{S3_BUCKET_GOLD}/{DATA_LAKE_PROBABILITIES_RATES_LOOKUP}"
ABLAUF_CSV_S3 = f"s3://{S3_BUCKET_GOLD}/{DATA_LAKE_PROBABILITIES_RATES_ABLAUF}"

# Explorer-facing intensity names (m / m/s / kN/m²) → gold columns (cm / cm/s / kPa).
# kPa == kN/m², so Druck needs no conversion; the cm columns divide by 100.
_GOLD_COL   = {'depth': 'Fliesstiefe', 'velocity': 'Fliessgeschwindigkeit', 'pressure': 'Druck'}
_TO_DISPLAY = {'depth': 1 / 100.0, 'velocity': 1 / 100.0, 'pressure': 1.0}

# p_h has no lookup: gold carries h and d (bodengruendigkeit) on every row.
# _p_h_sql/_lambda_ereignis_sql take optional p_h_mean/p_h_max -- expert mode
# (probe_explorer's "Experten-Modus", decision 2026-08-20): a client can
# override the two global weights (and, via ablauf_override/anriss_overrides
# elsewhere in this module, p_Ablauf and per-id_anriss lambda_Hangmuren/
# p_raeumlich/p_A) for an ad-hoc what-if recompute. None means "use the
# production default" -- every call site defaults to None, so normal
# (non-expert) behavior is byte-for-byte unchanged from before expert mode
# existed.
def _p_h_sql(p_h_mean: float | None = None, p_h_max: float | None = None) -> str:
    mean = GOLD_P_H_MEAN if p_h_mean is None else p_h_mean
    max_ = GOLD_P_H_MAX if p_h_max is None else p_h_max
    return f"(CASE WHEN dg.h = dg.d THEN {mean} ELSE {max_} END)"


def _lambda_ereignis_sql(p_h_mean: float | None = None, p_h_max: float | None = None) -> str:
    # lambda_ereignis should never be NULL or negative in gold-joined data, and
    # (with no expert-mode override, and outside the lambda_Hangmuren=0
    # exception) never exactly zero either. The only known way to get
    # p_raeumlich == 0 is bodengruendigkeit (d) == 0 (confirmed row-level
    # across all 61.3M Xurce rows, zero exceptions) -- but those 5,091
    # anrisse are dropped from the SIMULATION manifest entirely, before any
    # simulation runs (pre_processing_MAXI.py's `WHERE bodengruendigkeit >
    # 0`), so their id_anriss can never appear as a gold row to join against
    # in the first place; p_h and p_ablauf are never 0 either in the
    # production defaults (p_ablauf's minimum is ~7.8e-4). So a zero/
    # negative lambda_ereignis surviving this join, for any OTHER reason, means
    # some invariant broke upstream -- see _assert_valid_lambda_ereignis, which
    # fails loudly on that rather than silently dropping it. Two known
    # legitimate exceptions to "never exactly zero": an active expert-mode
    # override (a client CAN legitimately drive a factor to exactly 0, e.g.
    # p_h_max=0 -- allow_zero) and lambda_Hangmuren=0, confirmed real in the
    # current Xurce delivery for 52 prozessquellen / 8,114 real simulated
    # id_anriss (2026-09-11, see _assert_valid_lambda_ereignis's docstring) --
    # both tolerated by _assert_valid_lambda_ereignis without weakening the NULL
    # check or the "anything else" zero check.
    return f'pl.lambda_Hangmuren * pl.p_raeumlich * pl."p_A" * {_p_h_sql(p_h_mean, p_h_max)} * ab.p_ablauf'

# ── Raster grid constants + helpers (formerly shared with build_raster.py,
# MIDI-era, removed 2026-07-18 — these are schema-agnostic DataFrame→GeoTIFF
# plumbing, so they moved here verbatim rather than being recreated) ──────────
INTENSITY_VARS   = ['depth', 'velocity', 'pressure']
INTENSITY_UNITS  = {'depth': 'm', 'velocity': 'm/s', 'pressure': 'kN/m²'}
INTENSITY_SUFFIX = {'depth': 'm', 'velocity': 'ms', 'pressure': 'kNm2'}

RESOLUTION = 5.0
CRS_EPSG   = 2056
NODATA     = np.float32('nan')


def assert_lv95_grid_phase(coords, *, edge: bool, resolution: float = RESOLUTION,
                            atol: float = 1e-6, context: str = "") -> None:
    """Raise if any value in `coords` isn't on this project's LV95 grid at
    the expected phase -- every raster here is nominally 5x5m EPSG:2056
    with pixel CENTERS at k*resolution + resolution/2 (e.g. ...2.5/...7.5),
    never a round multiple; a pixel EDGE (what rasterio.transform.
    from_origin() wants) is the opposite -- a round multiple of resolution.
    Silently mixing the two up doesn't crash: it just misaligns the raster,
    or worse, makes every pixel index round-ambiguous and collides adjacent
    cells (2026-09-10: this exact mistake -- reusing readRasterHeader's
    xllcenter/yllcenter, themselves pixel CENTERS, as if they were a raster
    EDGE for from_origin() -- silently discarded ~70% of a Gebäudeschatten
    affected-mask raster via np.round's round-half-to-even, with no error;
    see docs/claude-memory/project_lv95_grid_phase_bug.md). Call this at any
    from_origin()/index-conversion chokepoint whose origin comes from
    something other than a fresh `df['x'].min()`-style real-data value
    (those are always already center-phase by construction)."""
    expected = 0.0 if edge else resolution / 2.0
    arr = np.atleast_1d(np.asarray(coords, dtype=float))
    offset = np.abs(arr % resolution - expected)
    offset = np.minimum(offset, resolution - offset)
    bad = offset > atol
    if np.any(bad):
        phase_name = "edge (round multiple of resolution)" if edge else f"center (k*resolution + {expected:g})"
        raise ValueError(
            f"{context + ': ' if context else ''}{int(bad.sum())}/{arr.size} coordinate(s) not on "
            f"the LV95 grid's {phase_name} phase (resolution={resolution:g}m) -- sample offenders: "
            f"{arr[bad][:5].tolist()}. This project's rasters are always {resolution:g}x{resolution:g}m "
            f"LV95 -- a center/edge mixup here silently misaligns or corrupts the raster.")


# Sanity ceiling for _make_grid's width/height -- generous enough to cover
# a canton-wide mosaic (the full DEM bbox is ~25200x23600 px at 5m, see
# hilbert_bern.py's docstring) with real margin, but tight enough to catch
# garbage x/y values from upstream data corruption fast: a raw OverflowError
# ("Python int too large to convert to C long") from rasterio.open sizing an
# absurd array is cryptic and gives no hint it's a DATA problem, not a
# rasterio one (2026-08-25: hit for real from a DuckDB temp-directory spill
# collision corrupting a kachel's x/y values -- see build_raster_derivate.py's
# worker_temp_dir docstring for the actual bug; this check doesn't fix that
# class of corruption, it just fails loud and fast instead of cryptically).
_MAX_GRID_DIM_PX = 100_000


def _make_grid(df: pd.DataFrame):
    res  = RESOLUTION
    xmin = float(df['x'].min())
    xmax = float(df['x'].max())
    ymin = float(df['y'].min())
    ymax = float(df['y'].max())
    width  = round((xmax - xmin) / res) + 1
    height = round((ymax - ymin) / res) + 1
    if width > _MAX_GRID_DIM_PX or height > _MAX_GRID_DIM_PX or width < 1 or height < 1:
        raise ValueError(
            f"_make_grid: computed grid {width}x{height} px is outside the sane "
            f"[1, {_MAX_GRID_DIM_PX}] range (x: [{xmin}, {xmax}], y: [{ymin}, {ymax}]) -- "
            f"almost certainly corrupted/garbage x/y values upstream, not a real raster size."
        )
    col_idx = np.round((df['x'].values - xmin) / res).astype(np.int32)
    row_idx = np.round((ymax - df['y'].values) / res).astype(np.int32)
    transform = from_origin(xmin - res / 2, ymax + res / 2, res, res)
    return width, height, col_idx, row_idx, transform


def _write_single_band(df: pd.DataFrame, value_col: str, out_path: str, tags: dict) -> None:
    width, height, col_idx, row_idx, transform = _make_grid(df)
    arr = np.full((height, width), np.nan, dtype=np.float32)
    arr[row_idx, col_idx] = df[value_col].values.astype(np.float32)

    profile = {
        'driver': 'COG', 'dtype': 'float32',
        'width': width, 'height': height, 'count': 1,
        'crs': CRS.from_epsg(CRS_EPSG), 'transform': transform,
        'nodata': NODATA, 'compress': 'deflate',
        'blocksize': 512, 'overview_resampling': 'average',
    }
    with rasterio.open(out_path, 'w', **profile) as dst:
        dst.write(arr, 1)
        dst.update_tags(1, **tags)
    print(f"  → {out_path}  ({width}×{height} px, {df[value_col].notna().sum():,} pixels with data)")


def _write_multiband(df: pd.DataFrame, value_cols: list[str], out_path: str, tags: dict) -> None:
    """Same grid/profile as _write_single_band, one band per column in
    value_cols (band N's description set to that column's name, for GIS
    tools that display it) -- used by derivate/build_raster_derivate.py to
    write one GeoTIFF per kachel covering its whole precompute matrix,
    instead of one file per (mode, variable, threshold/RP) combo."""
    width, height, col_idx, row_idx, transform = _make_grid(df)
    profile = {
        'driver': 'COG', 'dtype': 'float32',
        'width': width, 'height': height, 'count': len(value_cols),
        'crs': CRS.from_epsg(CRS_EPSG), 'transform': transform,
        'nodata': NODATA, 'compress': 'deflate',
        'blocksize': 512, 'overview_resampling': 'average',
    }
    with rasterio.open(out_path, 'w', **profile) as dst:
        for i, col in enumerate(value_cols, start=1):
            arr = np.full((height, width), np.nan, dtype=np.float32)
            arr[row_idx, col_idx] = df[col].values.astype(np.float32)
            dst.write(arr, i)
            dst.set_band_description(i, col)
        dst.update_tags(**tags)
    print(f"  → {out_path}  ({width}×{height} px, {len(value_cols)} band(s))")


def kachel_s3_file(id_kachel: int) -> str:
    return f"{GOLD_S3_ROOT}/id_kachel={int(id_kachel)}/data.parquet"


def kacheln_for_bbox(xmin, ymin, xmax, ymax) -> list[int]:
    """All id_kachel integers whose 1 km tile overlaps the bbox."""
    return [e * 10000 + n
            for e in range(int(xmin // 1000), int(xmax // 1000) + 1)
            for n in range(int(ymin // 1000), int(ymax // 1000) + 1)]


# ── Selection: a rectangle OR an arbitrary polygon, unified ──────────────────
# A drawn selection is always {'type': 'bbox'|'polygon', 'ring': [[E, N], ...]}
# (closed LV95 ring, last point == first) -- a plain rectangle is just the
# 4-corner special case of the same shape, so kachel selection (always the
# bounding rectangle -- gold is only ever fetched per whole 1 km tile
# regardless of the exact clip shape, see kacheln_for_bbox) and the per-pixel
# clip (exact for 'bbox', point-in-polygon for 'polygon', see _bind_gold_tile)
# both derive from this one representation instead of two parallel code
# paths.

def rect_ring(xmin, ymin, xmax, ymax) -> list[list[float]]:
    return [[xmin, ymin], [xmax, ymin], [xmax, ymax], [xmin, ymax], [xmin, ymin]]


def normalize_selection(selection) -> dict:
    """Accepts either the unified dict shape or a bare (xmin, ymin, xmax,
    ymax) 4-tuple (kept for convenience -- every internal caller so far
    always uses a real bbox, never a polygon, and forcing them all to
    hand-build the dict just to say "it's a rectangle" would be pure
    boilerplate)."""
    if isinstance(selection, dict):
        return selection
    xmin, ymin, xmax, ymax = selection
    return {'type': 'bbox', 'ring': rect_ring(xmin, ymin, xmax, ymax)}


def selection_bounds(selection) -> tuple[float, float, float, float]:
    ring = normalize_selection(selection)['ring']
    xs = [p[0] for p in ring]
    ys = [p[1] for p in ring]
    return min(xs), min(ys), max(xs), max(ys)


def _ring_wkt(ring) -> str:
    pts = ', '.join(f'{x} {y}' for x, y in ring)
    return f'POLYGON(({pts}))'


def _selection_tags(selection, xmin, ymin, xmax, ymax) -> dict:
    """GeoTIFF tags describing the requested clip -- always the bounding
    envelope, plus the exact polygon WKT when the clip isn't just that
    envelope (so the file is self-describing without needing the app's own
    session state to know what was actually requested)."""
    tags = {'bbox': f'{xmin},{ymin},{xmax},{ymax}'}
    if selection['type'] == 'polygon':
        tags['polygon_wkt'] = _ring_wkt(selection['ring'])
    return tags


def _is_missing_kachel(exc: Exception) -> bool:
    """S3 404 == kachel not finalized (or doesn't exist) — same defensive
    pattern data_lake/data_interface.py uses (e.g. get_anriss_sim_data)."""
    return isinstance(exc, duckdb.HTTPException) and "404" in str(exc)


def _assert_valid_lambda_ereignis(con: duckdb.DuckDBPyConnection, table_sql: str, context: str,
                             allow_zero: bool = False) -> None:
    """Raise loudly if any row has a NULL lambda_ereignis, or (unless allow_zero
    or lambda_Hangmuren=0) a zero/negative one -- see _lambda_ereignis_sql's
    comment for why NULL and zero/negative should both be impossible in the
    production default path OTHERWISE. NULL is always checked regardless of
    allow_zero: it indicates a broken join (missing probability_lookup/
    ablauf match), never a probability VALUE choice, so an active override
    never excuses it. allow_zero=True (set by callers when an expert-mode
    override is active) tolerates any exact 0 -- a client can legitimately
    drive a factor to 0 (e.g. p_h_max=0), but negative never has a
    legitimate cause either way.

    lambda_Hangmuren=0 (2026-09-11, see docs/claude-memory/
    project_maxi_delivery_qa.md): a SECOND, always-on legitimate zero cause,
    independent of allow_zero/expert-mode. Confirmed real in the current
    Xurce delivery -- 52 prozessquellen / 8,114 real, simulated id_anriss
    genuinely deliver lambda_Hangmuren=0 (a real process-source recurrence
    probability of zero, not a data defect), which zeroes the whole
    lambda_Ereignis product for every one of their pixels. table_sql MUST project
    both lambda_ereignis and lambda_Hangmuren for this to be checked -- a query
    that omits lambda_Hangmuren makes every zero row fail this assertion
    (fails closed, not open, on a caller mistake). Deliberately NOT handled
    by removing these id_anriss from probability_lookup.parquet instead:
    enrich_with_probabilities() (probe_explorer's single-anriss Rohdaten
    export) is exhaustive over its input and hard-fails on any id_anriss
    missing from the lookup -- removing them would fix this assertion but
    break that raw-data path for exactly these anrisse. Keeping them in the
    lookup with their real delivered (zero) value, and only relaxing the
    validation, keeps raw/single-anriss lookups working (correctly showing
    lambda_Ereignis=0) while every probability-WEIGHTED aggregate (IFK curves,
    Mode A/B rasters, the raster-derivate batch) naturally excludes their
    contribution anyway -- a 0-weight row already contributes nothing to a
    weighted sum, no separate filter needed anywhere."""
    if allow_zero:
        cond = "lambda_ereignis IS NULL OR lambda_ereignis < 0"
    else:
        cond = "lambda_ereignis IS NULL OR lambda_ereignis < 0 OR (lambda_ereignis = 0 AND lambda_Hangmuren != 0)"
    n = con.execute(f"SELECT COUNT(*) FROM ({table_sql}) WHERE {cond}").fetchone()[0]
    if n:
        kind = "NULL/negative" if allow_zero else "NULL/zero(non-lambda)/negative"
        raise ValueError(f"{context}: {n} row(s) with {kind} lambda_ereignis — investigate before "
                          f"proceeding (see docs/claude-memory/project_maxi_delivery_qa.md)")


_ABLAUF_SUM_TOLERANCE = 1e-6


def _register_ablauf(con: duckdb.DuckDBPyConnection, ablauf_override: pd.DataFrame | None = None) -> None:
    """Registers the `ablauf` table every lambda_Ereignis join reads. Expert mode:
    ablauf_override is a client-edited replacement for the full 36-row
    table, in which case it's used instead of the S3 CSV. Validated to still
    sum to 1 here (not just wherever the UI itself validates it) since this
    function is also directly callable from a script/API caller, not only
    through the explorer -- the invariant has to hold regardless of caller."""
    if ablauf_override is not None:
        _sum = float(ablauf_override['p_ablauf'].sum())
        if abs(_sum - 1.0) > _ABLAUF_SUM_TOLERANCE:
            raise ValueError(f"ablauf_override: p_ablauf sums to {_sum!r}, not 1.0")
        con.register('ablauf', ablauf_override)
    else:
        con.execute(f"CREATE OR REPLACE TEMP VIEW ablauf AS SELECT * FROM read_csv('{ABLAUF_CSV_S3}')")


def load_default_ablauf() -> pd.DataFrame:
    """The production p_Ablauf table as a plain DataFrame -- used by
    probe_explorer's Experten-Modus to seed its editable copy (same S3 read
    path as _register_ablauf's default branch, just returned as a
    DataFrame instead of bound into a connection)."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    configure_s3_for_duckdb(con)
    df = con.execute(f"SELECT * FROM read_csv('{ABLAUF_CSV_S3}')").df()
    con.close()
    return df


def _bind_probability_lookup(con: duckdb.DuckDBPyConnection, id_anriss_source_sql: str) -> None:
    """Probability lookup reduced to just the touched id_anriss (see module
    docstring — the full lookup is ~61M rows, one kachel/pixel touches only
    a handful to a few thousand)."""
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE probability_lookup AS
        SELECT pl.* FROM read_parquet('{PROBABILITY_LOOKUP_S3}') pl
        JOIN ({id_anriss_source_sql}) k USING (id_anriss)
    """)


def _apply_anriss_overrides(con: duckdb.DuckDBPyConnection, anriss_overrides: dict | None) -> None:
    """Patches the already-bound `probability_lookup` temp table (see
    _bind_probability_lookup, must run first) with client-supplied
    per-id_anriss overrides. Expert mode, IFK view only -- compute_ifk_
    default's anriss_events is already scoped to the handful of anrisse
    contributing to ONE pixel; build_raster_for_bbox never calls this (a
    raster can touch thousands of distinct id_anriss, too many to scope a
    client edit to sensibly, decision 2026-08-20).

    anriss_overrides: {id_anriss: {'lambda_Hangmuren': float|None,
    'p_raeumlich': float|None, 'p_A': float|None}} -- any factor left
    out/None keeps its production value.

    Built with pandas nullable 'Float64' dtype, not plain float64: plain
    float64's NaN is NOT the same as SQL NULL to DuckDB (NaN survives a
    COALESCE unchanged; only a real SQL NULL gets replaced), so a 'this
    field wasn't overridden' value has to round-trip as NULL, not NaN, or
    COALESCE below would silently keep the NaN instead of falling back to
    the production default."""
    if not anriss_overrides:
        return
    rows = [{'id_anriss': k, 'lambda_Hangmuren': v.get('lambda_Hangmuren'),
             'p_raeumlich': v.get('p_raeumlich'), 'p_A': v.get('p_A')}
            for k, v in anriss_overrides.items()]
    ov_df = pd.DataFrame(rows).astype({
        'lambda_Hangmuren': 'Float64', 'p_raeumlich': 'Float64', 'p_A': 'Float64'})
    con.register('_anriss_overrides', ov_df)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE probability_lookup AS
        SELECT pl.id_anriss,
               COALESCE(ov.lambda_Hangmuren, pl.lambda_Hangmuren) AS lambda_Hangmuren,
               COALESCE(ov.p_raeumlich, pl.p_raeumlich)           AS p_raeumlich,
               COALESCE(ov."p_A", pl."p_A")                       AS "p_A"
        FROM probability_lookup pl
        LEFT JOIN _anriss_overrides ov USING (id_anriss)
    """)
    con.unregister('_anriss_overrides')


def _expert_tags(p_h_mean: float | None, p_h_max: float | None,
                 ablauf_override: pd.DataFrame | None) -> dict:
    """GeoTIFF tags marking expert-mode (probability-override) raster output
    as non-official -- {} when no override is active, so a normal raster's
    tags are completely unaffected."""
    if p_h_mean is None and p_h_max is None and ablauf_override is None:
        return {}
    tags = {'expert_mode': 'true',
            'expert_mode_warning': 'NICHT OFFIZIELL -- angepasste Wahrscheinlichkeiten (Experten-Modus)'}
    if p_h_mean is not None:
        tags['expert_p_h_mean'] = str(p_h_mean)
    if p_h_max is not None:
        tags['expert_p_h_max'] = str(p_h_max)
    if ablauf_override is not None:
        tags['expert_ablauf_overridden'] = 'true'
    return tags


# ── IFK (default scenario) ────────────────────────────────────────────────────

def compute_ifk_default(x: float, y: float, *, p_h_mean: float | None = None,
                        p_h_max: float | None = None, ablauf_override: pd.DataFrame | None = None,
                        anriss_overrides: dict | None = None) -> dict:
    """IFK curves for one LV95 pixel from its kachel file.

    Returns {'pixel', 'curves': {depth|velocity|pressure: df(intensity,
    p_exceedance, return_period)}, 'anriss_events': df, 'ereignisse': df,
    'expert_mode': bool}. Intensities are in display units (m, m/s, kN/m²).

    Expert-mode overrides (probe_explorer's "Experten-Modus", all optional,
    default None = production behavior, byte-for-byte unchanged from before
    expert mode existed): p_h_mean/p_h_max override the two global
    GOLD_P_H_MEAN/MAX weights, ablauf_override replaces the full p_Ablauf
    table (see _register_ablauf), anriss_overrides patches specific
    id_anriss's lambda_Hangmuren/p_raeumlich/p_A (see
    _apply_anriss_overrides). When any override is active, an exact-zero
    (but not NULL or negative) lambda_ereignis is tolerated rather than raising —
    see _assert_valid_lambda_ereignis's allow_zero.
    """
    _has_overrides = bool(p_h_mean is not None or p_h_max is not None
                          or ablauf_override is not None or anriss_overrides)
    id_kachel = int(x // 1000) * 10000 + int(y // 1000)
    file = kachel_s3_file(id_kachel)
    empty = pd.DataFrame(columns=['intensity', 'p_exceedance', 'return_period'])
    result = {'pixel': (x, y),
              'curves': {v: empty.copy() for v in INTENSITY_VARS},
              'anriss_events': pd.DataFrame(columns=['id_anriss', 'x', 'y', 'anrissflaeche', 'lambda_ereignis_sum']),
              'ereignisse': pd.DataFrame(),
              'expert_mode': _has_overrides}

    con = _s3_gold_connection()
    _register_ablauf(con, ablauf_override)
    try:
        con.execute(f"""
            CREATE TEMP TABLE gold_pixel AS
            SELECT * FROM read_parquet('{file}') WHERE x = {x} AND y = {y}
        """)
    except duckdb.HTTPException as e:
        if _is_missing_kachel(e):
            con.close()
            return result
        raise
    _bind_probability_lookup(con, "SELECT DISTINCT id_anriss FROM gold_pixel")
    _apply_anriss_overrides(con, anriss_overrides)

    con.execute(f"""
        CREATE TEMP TABLE ereignisse AS
        SELECT dg.*,
               dg.Fliesstiefe / 100.0            AS depth,
               dg.Fliessgeschwindigkeit / 100.0  AS velocity,
               dg."Druck"                        AS pressure,
               pl.lambda_Hangmuren,
               pl.p_raeumlich,
               pl."p_A",
               {_p_h_sql(p_h_mean, p_h_max)}     AS p_h,
               ab.p_ablauf,
               {_lambda_ereignis_sql(p_h_mean, p_h_max)} AS lambda_ereignis
        FROM gold_pixel dg
        JOIN probability_lookup pl USING (id_anriss)
        JOIN ablauf ab ON dg.mu = ab.mu AND dg.xsi = ab.xsi AND dg.tau0 = ab.tau0
    """)
    _assert_valid_lambda_ereignis(con, "SELECT lambda_ereignis, lambda_Hangmuren FROM ereignisse", f"pixel ({x}, {y})",
                             allow_zero=_has_overrides)

    for var in INTENSITY_VARS:
        col, f = _GOLD_COL[var], _TO_DISPLAY[var]
        # Aggregate on the stored (exact) values, convert to display units after.
        df = con.execute(f"""
            WITH agg AS (
                SELECT "{col}" AS raw_intensity, SUM(lambda_ereignis) AS p_sum
                FROM ereignisse GROUP BY 1
            )
            SELECT raw_intensity * {f} AS intensity,
                   SUM(p_sum) OVER (ORDER BY raw_intensity DESC
                                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS p_exceedance
            FROM agg ORDER BY intensity
        """).df()
        df['return_period'] = 1.0 / df['p_exceedance']
        result['curves'][var] = df

    result['anriss_events'] = con.execute("""
        SELECT id_anriss, x_anriss AS x, y_anriss AS y, "A" AS anrissflaeche,
               COALESCE(SUM(lambda_ereignis), 0.0) AS lambda_ereignis_sum
        FROM ereignisse GROUP BY 1, 2, 3, 4 ORDER BY lambda_ereignis_sum DESC
    """).df()
    result['ereignisse'] = con.execute("""
        SELECT id_prozessquelle, id_anriss, x_anriss, y_anriss, "A", d, h, mu, xsi, tau0,
               lambda_Hangmuren, p_raeumlich, "p_A", p_h, p_ablauf,
               depth, velocity, pressure, lambda_ereignis
        FROM ereignisse ORDER BY id_anriss, "A", h, mu, xsi, tau0
    """).df()
    con.close()
    return result


def enrich_with_probabilities(df: pd.DataFrame) -> pd.DataFrame:
    """Join lambda_Ereignis (and its lambda_Hangmuren/p_raeumlich/p_A/p_h/p_Ablauf
    factors) onto an arbitrary in-memory SIM gold DataFrame -- the same
    probability_lookup + ablauf join compute_ifk_default uses for one pixel
    (_bind_probability_lookup/_register_ablauf/_p_h_sql/_lambda_ereignis_sql),
    reused here rather than duplicated so probe_explorer/app.py's
    single-anriss "Rohdaten" download (every pixel/parameter-combo row of one
    id_anriss, not just one pixel) computes lambda_Ereignis the exact same way the
    IFK curves do.

    Requires id_anriss, mu, xsi, tau0, h, d columns (GOLD_SCHEMA_SIM_DUCKDB --
    what get_anriss_sim_data/get_anriss_all_scenarios_gold_data return).
    Unlike compute_ifk_default's kachel-scoped joins, this is meant to be
    exhaustive over its input -- every input row is expected to find both a
    probability_lookup entry AND an ablauf combo, so (unlike the plain inner
    joins elsewhere in this module) it raises loudly if any row WOULD be
    dropped, instead of dropping it, and raises if any surviving row still
    ends up with a NULL/zero/negative lambda_ereignis (see
    _assert_valid_lambda_ereignis). No expert-mode override parameters -- this
    feeds the single-anriss Rohdaten export, not the IFK/raster expert-mode
    surfaces (see compute_ifk_default/build_raster_for_bbox)."""
    _extra_cols = ['lambda_Hangmuren', 'p_raeumlich', 'p_A', 'p_h', 'p_ablauf', 'lambda_ereignis']
    if df.empty:
        return df.assign(**{c: pd.Series(dtype='float64') for c in _extra_cols})

    con = _s3_gold_connection()
    _register_ablauf(con)
    con.register('gold_rows', df)
    _bind_probability_lookup(con, "SELECT DISTINCT id_anriss FROM gold_rows")

    missing_anriss = con.execute("""
        SELECT DISTINCT id_anriss FROM gold_rows
        ANTI JOIN probability_lookup USING (id_anriss)
    """).df()['id_anriss'].tolist()
    if missing_anriss:
        raise ValueError(f"enrich_with_probabilities: {len(missing_anriss)} id_anriss not found in "
                          f"probability_lookup: {missing_anriss}")
    missing_ablauf = con.execute("""
        SELECT DISTINCT mu, xsi, tau0 FROM gold_rows dg
        ANTI JOIN ablauf ab ON dg.mu = ab.mu AND dg.xsi = ab.xsi AND dg.tau0 = ab.tau0
    """).df()
    if not missing_ablauf.empty:
        raise ValueError(f"enrich_with_probabilities: {len(missing_ablauf)} (mu, xsi, tau0) combo(s) not "
                          f"found in ablauf: {missing_ablauf.to_dict('records')}")

    result = con.execute(f"""
        SELECT dg.*,
               pl.lambda_Hangmuren, pl.p_raeumlich, pl."p_A",
               {_p_h_sql()} AS p_h,
               ab.p_ablauf,
               {_lambda_ereignis_sql()} AS lambda_ereignis
        FROM gold_rows dg
        JOIN probability_lookup pl USING (id_anriss)
        JOIN ablauf ab ON dg.mu = ab.mu AND dg.xsi = ab.xsi AND dg.tau0 = ab.tau0
    """).df()
    con.close()
    # Same lambda_Hangmuren=0 exception as _assert_valid_lambda_ereignis (see its
    # docstring) -- a real, legitimate second zero-cause confirmed 2026-09-11,
    # not just this function's own separate implementation of the same check.
    invalid = result['lambda_ereignis'].isna() | (result['lambda_ereignis'] < 0) | \
        ((result['lambda_ereignis'] == 0) & (result['lambda_Hangmuren'] != 0))
    n_invalid = int(invalid.sum())
    if n_invalid:
        raise ValueError(f"enrich_with_probabilities: {n_invalid} row(s) with NULL/zero(non-lambda)/negative "
                          f"lambda_ereignis — investigate before proceeding (see docs/claude-memory/project_maxi_delivery_qa.md)")
    return result


# ── Rasters (default scenario, modes A/B as in build_raster.py) ───────────────

def _bind_gold_tile(con, id_kachel, selection) -> bool:
    """True if the kachel exists (finalized) and was bound; False if a 404
    (not finalized / doesn't exist) — callers skip it, same as an empty
    kachel used to be skipped when reading a local dir.

    The bounding-rectangle WHERE clause is always applied first (cheap, and
    matches row-group pruning on the leading (x, y) sort, see module
    docstring) -- for a 'polygon' selection, ST_Contains on top of that is an
    extra per-row filter for the sliver between the polygon and its own
    bounding box, not a replacement for it."""
    xmin, ymin, xmax, ymax = selection_bounds(selection)
    _extra_filter = ""
    if selection['type'] == 'polygon':
        _wkt = _ring_wkt(selection['ring'])
        _extra_filter = f" AND ST_Contains(ST_GeomFromText('{_wkt}'), ST_Point(x, y))"
    try:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE gold_tile AS
            SELECT * FROM read_parquet('{kachel_s3_file(id_kachel)}')
            WHERE x >= {xmin} AND x <= {xmax}
              AND y >= {ymin} AND y <= {ymax}
              {_extra_filter}
        """)
    except duckdb.HTTPException as e:
        if _is_missing_kachel(e):
            return False
        raise
    _bind_probability_lookup(con, "SELECT DISTINCT id_anriss FROM gold_tile")
    return True


def _raw_threshold(variable: str, threshold: float) -> float:
    """A display-unit threshold (m, m/s, kN/m²) in gold's stored unit (cm,
    cm/s, kPa), for comparing against the integer gold columns.

    Rounded to 6 decimals: the plain division carries floating-point noise
    (0.07 / 0.01 == 7.000000000000001), and `Fliesstiefe >= 7.000000000000001`
    silently drops the pixels at exactly 7 cm -- 18 of the 600 thresholds
    0.01 .. 6.00 were affected. 6 decimals is far below gold's resolution, so
    a threshold between two stored values (0.075 m -> 7.5 cm) still means
    what it says."""
    return round(threshold / _TO_DISPLAY[variable], 6)


def _curves_insert_sql() -> str:
    """Appends the current kachel's exceedance curves (from `base`, built and
    checked by build_exceedance_curves) to `curves`: per variable, pixel and
    stored intensity i, p = the summed lambda_ereignis of every event at that
    pixel with intensity >= i (gold's integer unit). Mode A is 1 / p at the
    smallest i >= the threshold, Mode B the largest i whose p reaches
    1 / return period -- so one curve answers every threshold and return
    period of all three variables."""
    branches = [f"""
        SELECT '{var}' AS variable, x, y, i,
               SUM(s) OVER (PARTITION BY x, y ORDER BY i DESC
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS p
        FROM (SELECT x, y, "{_GOLD_COL[var]}" AS i, SUM(lambda_ereignis) AS s
              FROM base GROUP BY x, y, "{_GOLD_COL[var]}")""" for var in INTENSITY_VARS]
    return "INSERT INTO curves" + "\n        UNION ALL".join(branches)


def _job_connection(job_dir: str) -> duckdb.DuckDBPyConnection:
    """A DuckDB database of its own in job_dir, spilling there.

    One folder per job: DuckDB names spill files by block size only
    (duckdb_temp_storage_S160K-0.tmp), so two jobs sharing one temp_directory
    -- two sessions of the same app pod -- overwrite and delete each other's:
    "IO Error: Could not read enough bytes from file .../duckdb_temp_storage_
    S160K-0.tmp" (2026-09-29)."""
    config = {
        'preserve_insertion_order': False,
        'temp_directory': job_dir,
        'memory_limit': safe_duckdb_memory_limit(),
        # threads from the cgroup quota, not the node's CPU count: 8 threads
        # against a 4-CPU pod limit buys no speed and doubles the number of
        # concurrent spill files.
        'threads': container_cpu_limit(),
    }
    # Bound the on-disk spill when the deployment says how much room there is.
    # DuckDB's default is "90% of available disk space", measured against the
    # NODE's filesystem -- it cannot see an emptyDir sizeLimit, so it spills
    # until the kubelet evicts the pod. Measured 2026-09-16 on the Hosttech
    # rehearsal: >21.6 GB for one bbox, pod evicted twice, job never finished
    # (NOTES.md, rehearsal finding 3). With the cap, an oversized job fails
    # with a clear DuckDB error instead of the pod being killed under it.
    max_temp = duckdb_max_temp_directory_size()
    if max_temp:
        config['max_temp_directory_size'] = max_temp
    # On-disk DB so DuckDB can spill (':memory:' ignores temp_directory).
    return duckdb.connect(os.path.join(job_dir, 'work.db'), config=config)


class _JobDir:
    """A private folder under RASTER_TEMP_DIR for one job's database and
    spill, deleted on exit. Entering also sweeps what killed jobs left."""

    def __enter__(self) -> str:
        # MAXI-specific name (not MIDI's '/tmp/duckdb_raster_temp'): MIDI runs
        # as root and creates that dir first with 0755 perms, which locks
        # bojan's MAXI process out of it entirely.
        os.makedirs(RASTER_TEMP_DIR, exist_ok=True)
        sweep_raster_temp(RASTER_TEMP_DIR)
        self.path = os.path.join(RASTER_TEMP_DIR, f'raster_work_{os.getpid()}_{uuid.uuid4().hex}')
        os.makedirs(self.path)
        return self.path

    def __exit__(self, *exc) -> None:
        shutil.rmtree(self.path, ignore_errors=True)


def build_exceedance_curves(selection, out_path: str, progress_callback=None, *,
                            p_h_mean: float | None = None, p_h_max: float | None = None,
                            ablauf_override: pd.DataFrame | None = None) -> None:
    """Reads the selection's gold rows kachel by kachel and writes their
    exceedance curves (see _curves_insert_sql) to the parquet out_path, which
    raster_from_curves turns into rasters. Written to a temp name and moved
    into place, so an existing out_path is always complete.

    Raises RasterBboxTooLargeError before any data is read if the selection
    is too large (check_raster_size). Expert-mode arguments as for
    build_raster_for_bbox; they change the curves, so a caller that keeps
    curves must key them by these too (curves_cache_key)."""
    selection = normalize_selection(selection)
    kacheln = kacheln_for_bbox(*selection_bounds(selection))
    check_raster_size(selection)
    has_overrides = p_h_mean is not None or p_h_max is not None
    print(f"\n[{datetime.now():%H:%M:%S}] Exceedance curves over {len(kacheln)} kacheln")
    tmp_path = f"{out_path}.{uuid.uuid4().hex}.tmp"
    with _JobDir() as job_dir:
        con = _job_connection(job_dir)
        try:
            con.execute("INSTALL httpfs; LOAD httpfs;")
            configure_s3_for_duckdb(con)
            if selection['type'] == 'polygon':
                # Only for an actual polygon clip (ST_Contains, _bind_gold_tile).
                con.execute("INSTALL spatial; LOAD spatial;")
            _register_ablauf(con, ablauf_override)
            con.execute("CREATE TABLE curves (variable VARCHAR, x DOUBLE, y DOUBLE, i INTEGER, p DOUBLE)")
            insert_sql = _curves_insert_sql()
            for n, id_kachel in enumerate(kacheln):
                if progress_callback:
                    progress_callback(n, len(kacheln), f"Kachel {n + 1}/{len(kacheln)}")
                if not _bind_gold_tile(con, id_kachel, selection):
                    continue
                # Materialised once: lambda_ereignis feeds three curves and the
                # validity check below.
                con.execute(f"""
                    CREATE OR REPLACE TEMP TABLE base AS
                    SELECT dg.x, dg.y, dg."Fliesstiefe", dg."Fliessgeschwindigkeit", dg."Druck",
                           pl.lambda_Hangmuren, {_lambda_ereignis_sql(p_h_mean, p_h_max)} AS lambda_ereignis
                    FROM gold_tile dg
                    JOIN probability_lookup pl USING (id_anriss)
                    JOIN ablauf ab ON dg.mu = ab.mu AND dg.xsi = ab.xsi AND dg.tau0 = ab.tau0
                """)
                _assert_valid_lambda_ereignis(con, "SELECT lambda_ereignis, lambda_Hangmuren FROM base",
                                              f"kachel {id_kachel}", allow_zero=has_overrides)
                con.execute(insert_sql)
            if progress_callback and kacheln:
                progress_callback(len(kacheln), len(kacheln), "Schreibe Kurven…")
            # Sorted by variable: raster_from_curves reads one variable at a
            # time and skips the other row groups.
            con.execute(f"COPY (SELECT * FROM curves ORDER BY variable) TO '{tmp_path}' "
                        f"(FORMAT parquet, COMPRESSION zstd)")
            os.replace(tmp_path, out_path)
        finally:
            con.close()
            if os.path.exists(tmp_path):
                os.remove(tmp_path)


def raster_from_curves(curves_path: str, selection, mode: str, *, variable: str = 'depth',
                       threshold: float = 1.0, return_period: float = 300, out_dir: str,
                       extra_tags: dict | None = None) -> list[str]:
    """The rasters of one request from the curves build_exceedance_curves
    wrote for this selection: Mode A one GeoTIFF (return period at
    `threshold` of `variable`), Mode B one per variable (intensity at
    `return_period`); a raster without any pixel is left out. File names and
    tags as build_raster_for_bbox documents them."""
    selection = normalize_selection(selection)
    xmin, ymin, xmax, ymax = selection_bounds(selection)
    bbox_slug = f"{int(xmin)}_{int(ymin)}_{int(xmax)}_{int(ymax)}"
    tags = {**_selection_tags(selection, xmin, ymin, xmax, ymax), **(extra_tags or {})}
    curves = f"read_parquet('{curves_path}')"
    with _JobDir() as job_dir:
        con = _job_connection(job_dir)
        try:
            if mode == 'a':
                df = con.execute(f"""
                    SELECT x, y, 1.0 / MAX(p) AS return_period FROM {curves}
                    WHERE variable = ? AND i >= ? GROUP BY x, y
                """, [variable, _raw_threshold(variable, threshold)]).df()
                if df.empty:
                    print("  No pixels exceed threshold — skipping.")
                    return []
                out_path = os.path.join(
                    out_dir, f'{variable}_at_{threshold}{INTENSITY_SUFFIX[variable]}_{bbox_slug}.tif')
                _write_single_band(df, 'return_period', out_path, {
                    'mode': 'A', 'variable': variable,
                    'threshold': str(threshold), 'threshold_units': INTENSITY_UNITS[variable],
                    'value_units': 'years (return period)', **tags,
                })
                return [out_path]
            p_thresh = 1.0 / return_period
            rp_str = str(int(return_period)) if float(return_period) == int(return_period) else str(return_period)
            out_paths = []
            for var in INTENSITY_VARS:
                df = con.execute(f"""
                    SELECT x, y, MAX(i) * {_TO_DISPLAY[var]!r} AS intensity FROM {curves}
                    WHERE variable = ? AND p >= ? GROUP BY x, y
                """, [var, p_thresh]).df()
                if df.empty:
                    print(f"  No pixels reach T={return_period} yr for {var} — skipping.")
                    continue
                out_path = os.path.join(out_dir, f'{var}_rp{rp_str}_{bbox_slug}.tif')
                _write_single_band(df, 'intensity', out_path, {
                    'mode': 'B', 'variable': var, 'value_units': INTENSITY_UNITS[var],
                    'return_period': rp_str, 'exceedance_probability': f'{p_thresh:.2e}', **tags,
                })
                out_paths.append(out_path)
            return out_paths
        finally:
            con.close()


# _run_mode_a/_run_mode_b pull each kachel's matching rows out of DuckDB via
# .df() and hold them in a plain Python list (`chunks`) for the WHOLE bbox
# loop, growing until the final pd.concat() -- that accumulation lives in
# pandas/Python memory, entirely outside whatever the DuckDB connection's own
# memory_limit tracks (see data_interface.safe_duckdb_memory_limit's
# docstring). 2026-08-16: a 42x31 km bbox (~1300 kacheln) grew the whole
# Streamlit process to ~14 GB RSS and got OOM-killed -- along with taking the
# entire WSL VM down with it, not just the one process. MAX_RASTER_KACHELN
# below is the actual fix for that failure mode: reject an oversized bbox
# outright, before opening a connection or reading a single kachel, rather
# than trying to survive one already in flight.
#
# _RASTER_KACHEL_MEMORY_BUDGET_BYTES is a deliberately pessimistic per-kachel
# worst case (not a measured average) -- a kachel's raw SIM_SPATIAL rows
# before aggregation can run into the hundreds of thousands (5 m pixels x
# every id_anriss/A/h/mu/xsi/tau0 combination reaching each one), and this
# cap has to hold even for an unusually dense kachel, not just a typical one.
_RASTER_KACHEL_MEMORY_BUDGET_BYTES = 25 * 1024 * 1024  # 25 MB/kachel, worst case
_RASTER_MAX_MEMORY_FRACTION = 0.25  # leave the rest of the host's RAM for the OS/Streamlit/everything else
_RASTER_MAX_KACHELN_CEILING = 300   # hard ceiling regardless of host RAM -- past this a single-threaded per-kachel Python loop is also just too slow to be a reasonable synchronous request


def max_raster_kacheln() -> int:
    """Safe upper bound on kacheln-per-request for build_raster_for_bbox,
    sized to the RAM this process may actually use (see
    container_memory_limit_bytes -- in a pod that is the cgroup limit, NOT the
    node's RAM that psutil reports) and clamped so the same code can't be
    talked into an unbounded job on a bigger box either."""
    total = container_memory_limit_bytes()
    by_memory = int(total * _RASTER_MAX_MEMORY_FRACTION / _RASTER_KACHEL_MEMORY_BUDGET_BYTES)
    return max(4, min(by_memory, _RASTER_MAX_KACHELN_CEILING))


class RasterBboxTooLargeError(ValueError):
    """Raised by build_raster_for_bbox before any work starts, see
    check_raster_size."""


# Gold's bytes per row, from the parquet footers of 16 kacheln of
# adelboden-test (2026-09-29): 3.0-3.9, 3.75 typical. A kachel's file size
# thus gives its row count without reading it -- and the rows read, not the
# kacheln touched, are what a raster costs: kacheln range from 0 to ~510 M rows.
_GOLD_BYTES_PER_ROW = 3.75
# Curve-build throughput (gold read from S3 + curves), measured 2026-09-29 on
# the WSL dev box (16 threads, 3.4 GB DuckDB memory) over 183 M rows of one
# dense kachel. A 4-CPU pod is slower; RASTER_MAX_ROWS_DEFAULT leaves room.
_RASTER_ROWS_PER_SECOND = 1.0e6
# Largest build allowed: ~15 min at the rate above -- one dense kachel is
# ~270-510 M rows. Beyond this a job blocks the app's single raster slot for
# too long (and spills tens of GB). PROBE_RASTER_MAX_ROWS overrides it.
RASTER_MAX_ROWS_DEFAULT = 900_000_000


def raster_max_rows() -> int:
    return int(os.getenv('PROBE_RASTER_MAX_ROWS') or RASTER_MAX_ROWS_DEFAULT)


@lru_cache(maxsize=1)
def gold_kachel_bytes() -> dict[int, int]:
    """File size of every gold kachel, from one S3 listing (no data read).
    Cached for the process: gold is immutable."""
    sizes = {}
    pages = get_s3_client().get_paginator('list_objects_v2').paginate(
        Bucket=S3_BUCKET_GOLD, Prefix=f"{DATA_LAKE_DIR_GOLD_SIM_SPATIAL}/id_kachel=")
    for page in pages:
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('/data.parquet'):
                sizes[int(obj['Key'].split('id_kachel=')[1].split('/')[0])] = obj['Size']
    return sizes


def estimate_raster_rows(selection) -> int:
    """Gold rows a build over the selection's bounding rectangle reads: each
    kachel's rows (file size / _GOLD_BYTES_PER_ROW) times the share of the
    kachel the rectangle covers, as if rows were spread evenly over it.
    Measured: a 0.8 x 0.8 km box (64 % of its kachel) read 68 % of the rows."""
    xmin, ymin, xmax, ymax = selection_bounds(normalize_selection(selection))
    sizes = gold_kachel_bytes()
    rows = 0.0
    for id_kachel in kacheln_for_bbox(xmin, ymin, xmax, ymax):
        e, n = divmod(id_kachel, 10000)
        share = (max(0.0, min(xmax, (e + 1) * 1000) - max(xmin, e * 1000))
                 * max(0.0, min(ymax, (n + 1) * 1000) - max(ymin, n * 1000)) / 1e6)
        rows += sizes.get(id_kachel, 0) / _GOLD_BYTES_PER_ROW * share
    return int(rows)


def estimate_raster_seconds(selection) -> float:
    """Rough wall-clock time of building the selection's curves, from
    estimate_raster_rows and _RASTER_ROWS_PER_SECOND."""
    return estimate_raster_rows(selection) / _RASTER_ROWS_PER_SECOND


def check_raster_size(selection) -> None:
    """Raises RasterBboxTooLargeError if the selection spans more kacheln
    than this host can hold (max_raster_kacheln) or more gold rows than
    raster_max_rows(). Both use the bounding rectangle: gold is fetched by
    rectangle, so a thin polygon costs what its rectangle costs."""
    xmin, ymin, xmax, ymax = selection_bounds(normalize_selection(selection))
    n_kacheln = len(kacheln_for_bbox(xmin, ymin, xmax, ymax))
    if n_kacheln > max_raster_kacheln():
        raise RasterBboxTooLargeError(
            f"Gebiet umfasst {n_kacheln} Kacheln (~{n_kacheln} km²) — mehr als das für dieses "
            f"System sichere Maximum von {max_raster_kacheln()}. Bitte ein kleineres Gebiet wählen.")
    rows, max_rows = estimate_raster_rows(selection), raster_max_rows()
    if rows > max_rows:
        raise RasterBboxTooLargeError(
            f"Gebiet enthält geschätzt {rows / 1e6:,.0f} Mio. Simulationspixel — mehr als das Maximum "
            f"von {max_rows / 1e6:,.0f} Mio. Bitte ein kleineres Gebiet wählen.".replace(",", "'"))


def curves_cache_key(selection, p_h_mean: float | None = None, p_h_max: float | None = None,
                     ablauf_override: pd.DataFrame | None = None) -> str:
    """Name for the curves of one selection and probability setting, for a
    caller that keeps them (build_raster_for_bbox's curves_path). Includes the
    ETags of the probability lookup and the p_Ablauf table on S3, so a new
    delivery gets new curves; gold itself is immutable."""
    s3 = get_s3_client()
    etags = [s3.head_object(Bucket=S3_BUCKET_GOLD, Key=key)['ETag']
             for key in (DATA_LAKE_PROBABILITIES_RATES_LOOKUP, DATA_LAKE_PROBABILITIES_RATES_ABLAUF)]
    ablauf = None if ablauf_override is None else ablauf_override.to_json(orient='split', double_precision=15)
    payload = json.dumps([_CURVES_FORMAT, normalize_selection(selection), p_h_mean, p_h_max, ablauf, etags],
                         sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()


# Bump when the curves' content or layout changes, so kept curves are rebuilt.
_CURVES_FORMAT = 1


# Deliberately wide range, not a single number -- and recalibrated 2026-08-17
# from a real 40-kachel benchmark (build_raster_for_bbox, mode B, one kachel
# per job) run on silver-to-gold-node, not on a dev laptop: that box (8 vCPU,
# same Hosttech Berlin DC as the S3 bucket) is the same class of machine
# app.py actually runs on in production (headnode: also 8 vCPU, also
# Hosttech Berlin) -- a comparison run from a Zurich dev box over the public
# internet measured noticeably slower on sparse/empty kacheln (network RTT
# dominates there) but came out roughly EQUAL on the densest kacheln (~350M+
# raw rows) despite the worse network path, because at that size the
# DuckDB join+aggregate itself becomes CPU-bound and the dev box's 16 cores
# outweighed its network disadvantage against the 8-core DC boxes -- so the
# dev box's numbers alone would have been a poor stand-in for how this
# actually performs where it's deployed.
#
# The real per-kachel spread measured on that DC box: empty/near-empty tiles
# ~1s, up to ~400s for the single densest tile (364M raw rows before
# aggregation). Kachel density varies 1000x+ across the lake (confirmed 0 to
# ~364M raw rows per km² tile), so a per-kachel constant will always read as
# far more precise than it can actually be -- LOW/HIGH below are ~p10/~p90
# of that real sample, not the full min/max, to keep the range from being
# dominated by the single most extreme outlier either direction. This is a
# rough sizing hint for the UI (see probe_explorer/app.py's Raster-Karten
# sidebar) shown only before a job has processed anything yet -- once it
# has, the sidebar switches to a live estimate from that job's own actual
# pace, which is always better than this static guess.
_RASTER_SECONDS_PER_KACHEL_LOW = 1.0
_RASTER_SECONDS_PER_KACHEL_HIGH = 245.0


def estimate_raster_duration_seconds(n_kacheln: int) -> tuple[float, float]:
    """Rough (low, high) wall-clock estimate in seconds for a
    build_raster_for_bbox job touching n_kacheln tiles -- see
    _RASTER_SECONDS_PER_KACHEL_LOW/HIGH for why this is a range, not a point
    estimate."""
    return (n_kacheln * _RASTER_SECONDS_PER_KACHEL_LOW, n_kacheln * _RASTER_SECONDS_PER_KACHEL_HIGH)


RASTER_TEMP_DIR = '/tmp/duckdb_raster_temp_maxi'
RASTER_TEMP_MAX_AGE_S = 7 * 24 * 3600


def sweep_raster_temp(tmp_dir: str = RASTER_TEMP_DIR, now: float | None = None) -> int:
    """Deletes every entry of tmp_dir older than RASTER_TEMP_MAX_AGE_S and
    returns how many. A job deletes its own folder when it ends, but not when
    its process is killed (a pod restart, an OOM kill), and on the stages
    /tmp is a volume that outlives the pod. Also takes the loose
    raster_work_*.db and spill files of versions before per-job folders.
    No running job is anywhere near that old."""
    cutoff = (time.time() if now is None else now) - RASTER_TEMP_MAX_AGE_S
    removed = 0
    for entry in os.scandir(tmp_dir):
        try:
            if entry.stat(follow_symlinks=False).st_mtime >= cutoff:
                continue
            if entry.is_dir(follow_symlinks=False):
                shutil.rmtree(entry.path, ignore_errors=True)
            else:
                os.remove(entry.path)
            removed += 1
        except OSError:
            continue  # another job's sweep got there first
    return removed


def build_raster_for_bbox(selection, mode, variable='depth', threshold=1.0,
                          return_period=300, out_dir=None,
                          progress_callback=None, *,
                          p_h_mean: float | None = None, p_h_max: float | None = None,
                          ablauf_override: pd.DataFrame | None = None,
                          curves_path: str | None = None) -> list[str]:
    """Default-scenario raster(s) for an LV95 area from the S3 gold lake.
    `selection` is either a plain (xmin, ymin, xmax, ymax) bbox tuple, or the
    unified {'type': 'bbox'|'polygon', 'ring': [[E, N], ...]} shape (see
    normalize_selection) for an arbitrary drawn polygon.

    Mode 'a': one GeoTIFF, the return period at which `variable` reaches
    `threshold` ({variable}_at_{threshold}{unit}_{bbox}.tif). Mode 'b': one
    GeoTIFF per variable, the intensity at `return_period`
    ({variable}_rp{T}_{bbox}.tif). Returns the paths written; a raster
    without any pixel is left out.

    Two steps: build_exceedance_curves reads gold (the slow part: minutes per
    dense kachel), raster_from_curves turns the curves into the rasters
    (seconds). With curves_path, curves already there are reused, and
    missing ones are built there and kept -- the caller owns that file, and
    must key it by curves_cache_key. Without it they go to a temp file.

    Raises RasterBboxTooLargeError (before any data is read) if the selection
    is too large to build, see check_raster_size.

    Expert-mode overrides (probe_explorer's "Experten-Modus"), default None
    = production behavior: p_h_mean/p_h_max override the two global
    GOLD_P_H_MEAN/MAX weights; ablauf_override replaces the full p_Ablauf
    table (see _register_ablauf). No per-anriss override parameter here
    (unlike compute_ifk_default) -- a raster can touch thousands of distinct
    id_anriss, too many to scope a client edit to sensibly (decision
    2026-08-20). Output GeoTIFFs get an `expert_mode` tag (see _expert_tags)
    when any override is active.
    """
    selection = normalize_selection(selection)
    out_dir = out_dir or os.path.expanduser('~/probe_data/maxi_rasters')
    os.makedirs(out_dir, exist_ok=True)
    owned = curves_path is None
    if owned:
        os.makedirs(RASTER_TEMP_DIR, exist_ok=True)
        curves_path = os.path.join(RASTER_TEMP_DIR, f'curves_{os.getpid()}_{uuid.uuid4().hex}.parquet')
    try:
        if not os.path.exists(curves_path):
            build_exceedance_curves(selection, curves_path, progress_callback, p_h_mean=p_h_mean,
                                    p_h_max=p_h_max, ablauf_override=ablauf_override)
        if progress_callback:
            progress_callback(1, 1, "Schreibe GeoTIFF…")
        return raster_from_curves(curves_path, selection, mode, variable=variable, threshold=threshold,
                                  return_period=return_period, out_dir=out_dir,
                                  extra_tags=_expert_tags(p_h_mean, p_h_max, ablauf_override))
    finally:
        if owned and os.path.exists(curves_path):
            os.remove(curves_path)
