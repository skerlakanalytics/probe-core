"""MAXI gold lake derivates: IFK curves and on-demand rasters, default scenario.

Reads the per-km² gold lake on S3 (Data-Lake-Gold/SIM_SPATIAL/
id_kachel=NNNNNNNN/data.parquet, schema GOLD_SCHEMA_SIM_DUCKDB). The kacheln
are the spatial index: a pixel or bbox query opens only the touched kachel
files, and row-group pruning on the leading (x, y) sort does the rest.

Gold carries no probabilities. They are joined at query time (_events_sql):

    lambda_Ereignis = lambda_Hangmuren · p_raeumlich · p_A · p_h · p_Ablauf

    lambda_Hangmuren, p_raeumlich, p_A   per id_anriss, from the probability lookup
                                         (Data-Lake-Probabilities-Rates/probability_lookup.parquet,
                                         built by the pipeline's derivate/build_probability_lookup.py)
    p_h                                  from gold's own h and d: h == d -> GOLD_P_H_MEAN,
                                         else GOLD_P_H_MAX
    p_Ablauf                             per (mu, xsi, tau0), from ablaufwahrscheinlichkeiten.csv
                                         (the 36 combinations of the MAXI grid, sums to 1)

A live join, not a materialized ENRICHED lake (decision 2026-08-13): the
factors are per-id_anriss scalars, so baking them in would repeat a tiny table
over billions of gold rows and force a lake rewrite on every correction. The
lookup has ~61M rows and is reduced to the touched id_anriss before joining
(_bind_probability_lookup); joining it whole ran out of memory.

History and rationale: probe_control_center's docs/claude-memory/
(project_gold_kachel_design.md, project_gebaeudeschatten_derivate_code_history.md).
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
_GOLD_COLUMN     = {'depth': 'Fliesstiefe', 'velocity': 'Fliessgeschwindigkeit', 'pressure': 'Druck'}
_TO_DISPLAY_UNIT = {'depth': 1 / 100.0, 'velocity': 1 / 100.0, 'pressure': 1.0}

# ── lambda_Ereignis: the one join every IFK curve and raster is built on ──────
# p_h has no lookup: gold carries h and d (bodengruendigkeit) on every row.
# The optional p_h_mean/p_h_max are expert mode (pgr-atlas's "Experten-Modus",
# decision 2026-08-20): a client can override the two global weights (and, via
# ablauf_override/anriss_overrides, p_Ablauf and per-id_anriss lambda_Hangmuren/
# p_raeumlich/p_A) for a what-if recompute. None = the production value.
def _p_h_sql(p_h_mean: float | None = None, p_h_max: float | None = None) -> str:
    """p_h of a gold row: h == d is the mean-thickness variant, anything else the max one."""
    mean = GOLD_P_H_MEAN if p_h_mean is None else p_h_mean
    max_ = GOLD_P_H_MAX if p_h_max is None else p_h_max
    return f"(CASE WHEN h = d THEN {mean} ELSE {max_} END)"


def _lambda_ereignis_sql(p_h_mean: float | None = None, p_h_max: float | None = None) -> str:
    """lambda_Ereignis of a gold row joined with probability_lookup and ablauf
    (see _events_sql). Columns are unqualified, so it works under any table
    alias. What values are legitimate: _assert_valid_lambda_ereignis."""
    return f'lambda_Hangmuren * p_raeumlich * "p_A" * {_p_h_sql(p_h_mean, p_h_max)} * p_ablauf'


def _events_sql(gold_table: str, p_h_mean: float | None = None, p_h_max: float | None = None) -> str:
    """SELECT every row of `gold_table` (one simulation at one pixel) with the
    probability factors of its event and their product, lambda_ereignis:

        gold row --id_anriss--------> probability_lookup  (lambda_Hangmuren, p_raeumlich, p_A)
                 --(mu, xsi, tau0)--> ablauf              (p_ablauf)
                 --its own h, d-----> p_h

    Needs the temp tables/views `probability_lookup` (_bind_probability_lookup)
    and `ablauf` (_register_ablauf). Inner joins: a row without a match in
    either drops out. p_h_mean/p_h_max: expert mode, None = production value."""
    return f"""
        SELECT gold.*,
               lookup.lambda_Hangmuren,
               lookup.p_raeumlich,
               lookup."p_A",
               {_p_h_sql(p_h_mean, p_h_max)} AS p_h,
               ablauf.p_ablauf,
               {_lambda_ereignis_sql(p_h_mean, p_h_max)} AS lambda_ereignis
        FROM {gold_table} AS gold
        JOIN probability_lookup AS lookup USING (id_anriss)
        JOIN ablauf USING (mu, xsi, tau0)
    """

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


# Largest plausible grid side. The canton-wide DEM is ~25200 x 23600 px at 5 m;
# anything beyond this is corrupted x/y upstream (2026-08-25: a DuckDB spill
# collision), reported as such instead of as rasterio's OverflowError.
_MAX_GRID_DIM_PX = 100_000


def _make_grid(pixels: pd.DataFrame):
    """(width, height, column index, row index, transform) of the smallest
    grid holding every pixel CENTER in `pixels` (columns x, y)."""
    resolution = RESOLUTION
    xmin = float(pixels['x'].min())
    xmax = float(pixels['x'].max())
    ymin = float(pixels['y'].min())
    ymax = float(pixels['y'].max())
    width  = round((xmax - xmin) / resolution) + 1
    height = round((ymax - ymin) / resolution) + 1
    if width > _MAX_GRID_DIM_PX or height > _MAX_GRID_DIM_PX or width < 1 or height < 1:
        raise ValueError(
            f"_make_grid: computed grid {width}x{height} px is outside the sane "
            f"[1, {_MAX_GRID_DIM_PX}] range (x: [{xmin}, {xmax}], y: [{ymin}, {ymax}]) -- "
            f"almost certainly corrupted/garbage x/y values upstream, not a real raster size."
        )
    column_index = np.round((pixels['x'].values - xmin) / resolution).astype(np.int32)
    row_index = np.round((ymax - pixels['y'].values) / resolution).astype(np.int32)
    transform = from_origin(xmin - resolution / 2, ymax + resolution / 2, resolution, resolution)
    return width, height, column_index, row_index, transform


def _geotiff_profile(width: int, height: int, band_count: int, transform) -> dict:
    return {
        'driver': 'COG', 'dtype': 'float32',
        'width': width, 'height': height, 'count': band_count,
        'crs': CRS.from_epsg(CRS_EPSG), 'transform': transform,
        'nodata': NODATA, 'compress': 'deflate',
        'blocksize': 512, 'overview_resampling': 'average',
    }


def _write_single_band(pixels: pd.DataFrame, value_column: str, out_path: str, tags: dict) -> None:
    width, height, column_index, row_index, transform = _make_grid(pixels)
    band = np.full((height, width), np.nan, dtype=np.float32)
    band[row_index, column_index] = pixels[value_column].values.astype(np.float32)
    with rasterio.open(out_path, 'w', **_geotiff_profile(width, height, 1, transform)) as dataset:
        dataset.write(band, 1)
        dataset.update_tags(1, **tags)
    print(f"  → {out_path}  ({width}×{height} px, {pixels[value_column].notna().sum():,} pixels with data)")


def _write_multiband(pixels: pd.DataFrame, value_columns: list[str], out_path: str, tags: dict) -> None:
    """Same grid and profile as _write_single_band, one band per column in
    value_columns (the band description is the column name) -- the derivate
    runner's one GeoTIFF per kachel and metric."""
    width, height, column_index, row_index, transform = _make_grid(pixels)
    profile = _geotiff_profile(width, height, len(value_columns), transform)
    with rasterio.open(out_path, 'w', **profile) as dataset:
        for band_number, column in enumerate(value_columns, start=1):
            band = np.full((height, width), np.nan, dtype=np.float32)
            band[row_index, column_index] = pixels[column].values.astype(np.float32)
            dataset.write(band, band_number)
            dataset.set_band_description(band_number, column)
        dataset.update_tags(**tags)
    print(f"  → {out_path}  ({width}×{height} px, {len(value_columns)} band(s))")


def kachel_s3_file(id_kachel: int) -> str:
    return f"{GOLD_S3_ROOT}/id_kachel={int(id_kachel)}/data.parquet"


def kacheln_for_bbox(xmin, ymin, xmax, ymax) -> list[int]:
    """All id_kachel integers whose 1 km tile overlaps the bbox."""
    return [east_km * 10000 + north_km
            for east_km in range(int(xmin // 1000), int(xmax // 1000) + 1)
            for north_km in range(int(ymin // 1000), int(ymax // 1000) + 1)]


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
    eastings = [point[0] for point in ring]
    northings = [point[1] for point in ring]
    return min(eastings), min(northings), max(eastings), max(northings)


def _ring_wkt(ring) -> str:
    points = ', '.join(f'{x} {y}' for x, y in ring)
    return f'POLYGON(({points}))'


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
    """Raise if any row of `table_sql` (must project lambda_ereignis and
    lambda_Hangmuren) has an impossible lambda_ereignis:

      NULL      always an error: a broken join, never a probability value.
      negative  always an error.
      zero      an error unless one of the two legitimate causes applies:
                - lambda_Hangmuren = 0: real in the Xurce delivery (52
                  prozessquellen / 8,114 simulated id_anriss, 2026-09-11, see
                  docs/claude-memory/project_maxi_delivery_qa.md). A query that
                  does not project lambda_Hangmuren fails here on every zero
                  row: closed, not open.
                - allow_zero: an expert-mode override is active, and a client
                  may set a factor to 0 (e.g. p_h_max=0).

    Why nothing else can be zero: p_raeumlich = 0 only happens with
    bodengruendigkeit = 0 (all 61.3M Xurce rows checked), and those 5,091
    anrisse are never simulated (pre_processing_MAXI.py), so they have no gold
    rows; p_h and p_ablauf are never 0 in production (p_ablauf >= ~7.8e-4).

    The lambda_Hangmuren = 0 anrisse stay in probability_lookup.parquet on
    purpose: enrich_with_probabilities hard-fails on an id_anriss missing from
    the lookup, and a rate of 0 adds nothing to any weighted sum anyway."""
    invalid = "lambda_ereignis IS NULL OR lambda_ereignis < 0"
    if not allow_zero:
        invalid += " OR (lambda_ereignis = 0 AND lambda_Hangmuren != 0)"
    n_invalid = con.execute(f"SELECT COUNT(*) FROM ({table_sql}) WHERE {invalid}").fetchone()[0]
    if n_invalid:
        kind = "NULL/negative" if allow_zero else "NULL/zero(non-lambda)/negative"
        raise ValueError(f"{context}: {n_invalid} row(s) with {kind} lambda_ereignis — investigate before "
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
        total = float(ablauf_override['p_ablauf'].sum())
        if abs(total - 1.0) > _ABLAUF_SUM_TOLERANCE:
            raise ValueError(f"ablauf_override: p_ablauf sums to {total!r}, not 1.0")
        con.register('ablauf', ablauf_override)
    else:
        con.execute(f"CREATE OR REPLACE TEMP VIEW ablauf AS SELECT * FROM read_csv('{ABLAUF_CSV_S3}')")


def load_default_ablauf() -> pd.DataFrame:
    """The production p_Ablauf table as a plain DataFrame -- used by
    pgr-atlas's Experten-Modus to seed its editable copy (same S3 read
    path as _register_ablauf's default branch, just returned as a
    DataFrame instead of bound into a connection)."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    configure_s3_for_duckdb(con)
    ablauf = con.execute(f"SELECT * FROM read_csv('{ABLAUF_CSV_S3}')").df()
    con.close()
    return ablauf


def _bind_probability_lookup(con: duckdb.DuckDBPyConnection, id_anriss_source_sql: str) -> None:
    """Probability lookup reduced to just the touched id_anriss (see module
    docstring — the full lookup is ~61M rows, one kachel/pixel touches only
    a handful to a few thousand)."""
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE probability_lookup AS
        SELECT lookup.*
        FROM read_parquet('{PROBABILITY_LOOKUP_S3}') AS lookup
        JOIN ({id_anriss_source_sql}) AS touched USING (id_anriss)
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
    factor_names = ['lambda_Hangmuren', 'p_raeumlich', 'p_A']
    overrides = pd.DataFrame([{'id_anriss': id_anriss, **{name: factors.get(name) for name in factor_names}}
                              for id_anriss, factors in anriss_overrides.items()])
    overrides = overrides.astype({name: 'Float64' for name in factor_names})
    con.register('_anriss_overrides', overrides)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE probability_lookup AS
        SELECT lookup.id_anriss,
               COALESCE(override.lambda_Hangmuren, lookup.lambda_Hangmuren) AS lambda_Hangmuren,
               COALESCE(override.p_raeumlich,      lookup.p_raeumlich)      AS p_raeumlich,
               COALESCE(override."p_A",            lookup."p_A")            AS "p_A"
        FROM probability_lookup AS lookup
        LEFT JOIN _anriss_overrides AS override USING (id_anriss)
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

    Expert-mode overrides (pgr-atlas's "Experten-Modus", all optional,
    default None = production behavior, byte-for-byte unchanged from before
    expert mode existed): p_h_mean/p_h_max override the two global
    GOLD_P_H_MEAN/MAX weights, ablauf_override replaces the full p_Ablauf
    table (see _register_ablauf), anriss_overrides patches specific
    id_anriss's lambda_Hangmuren/p_raeumlich/p_A (see
    _apply_anriss_overrides). When any override is active, an exact-zero
    (but not NULL or negative) lambda_ereignis is tolerated rather than raising —
    see _assert_valid_lambda_ereignis's allow_zero.
    """
    has_overrides = bool(p_h_mean is not None or p_h_max is not None
                          or ablauf_override is not None or anriss_overrides)
    id_kachel = int(x // 1000) * 10000 + int(y // 1000)
    kachel_file = kachel_s3_file(id_kachel)
    empty_curve = pd.DataFrame(columns=['intensity', 'p_exceedance', 'return_period'])
    result = {'pixel': (x, y),
              'curves': {variable: empty_curve.copy() for variable in INTENSITY_VARS},
              'anriss_events': pd.DataFrame(columns=['id_anriss', 'x', 'y', 'anrissflaeche', 'lambda_ereignis_sum']),
              'ereignisse': pd.DataFrame(),
              'expert_mode': has_overrides}

    con = _s3_gold_connection()
    _register_ablauf(con, ablauf_override)
    try:
        con.execute(f"""
            CREATE TEMP TABLE gold_pixel AS
            SELECT *
            FROM read_parquet('{kachel_file}')
            WHERE x = {x} AND y = {y}
        """)
    except duckdb.HTTPException as error:
        if _is_missing_kachel(error):
            con.close()
            return result
        raise
    _bind_probability_lookup(con, "SELECT DISTINCT id_anriss FROM gold_pixel")
    _apply_anriss_overrides(con, anriss_overrides)

    # Every simulation reaching the pixel, with its rate and display-unit intensities.
    con.execute(f"""
        CREATE TEMP TABLE ereignisse AS
        WITH events AS ({_events_sql("gold_pixel", p_h_mean, p_h_max)})
        SELECT *,
               Fliesstiefe / 100.0           AS depth,
               Fliessgeschwindigkeit / 100.0 AS velocity,
               "Druck"                       AS pressure
        FROM events
    """)
    _assert_valid_lambda_ereignis(con, "SELECT lambda_ereignis, lambda_Hangmuren FROM ereignisse", f"pixel ({x}, {y})",
                             allow_zero=has_overrides)

    for variable in INTENSITY_VARS:
        # The IFK curve: per intensity, the summed rate of every event at least
        # that intense. Grouped on the stored (exact) values, converted to
        # display units after.
        curve = con.execute(f"""
            WITH rate_per_intensity AS (
                SELECT "{_GOLD_COLUMN[variable]}" AS stored_intensity,
                       SUM(lambda_ereignis) AS rate
                FROM ereignisse
                GROUP BY stored_intensity
            )
            SELECT stored_intensity * {_TO_DISPLAY_UNIT[variable]} AS intensity,
                   SUM(rate) OVER (ORDER BY stored_intensity DESC
                                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS p_exceedance
            FROM rate_per_intensity
            ORDER BY intensity
        """).df()
        curve['return_period'] = 1.0 / curve['p_exceedance']
        result['curves'][variable] = curve

    result['anriss_events'] = con.execute("""
        SELECT id_anriss,
               x_anriss AS x,
               y_anriss AS y,
               "A"      AS anrissflaeche,
               COALESCE(SUM(lambda_ereignis), 0.0) AS lambda_ereignis_sum
        FROM ereignisse
        GROUP BY id_anriss, x_anriss, y_anriss, "A"
        ORDER BY lambda_ereignis_sum DESC
    """).df()
    result['ereignisse'] = con.execute("""
        SELECT id_prozessquelle, id_anriss, x_anriss, y_anriss, "A", d, h, mu, xsi, tau0,
               lambda_Hangmuren, p_raeumlich, "p_A", p_h, p_ablauf,
               depth, velocity, pressure, lambda_ereignis
        FROM ereignisse
        ORDER BY id_anriss, "A", h, mu, xsi, tau0
    """).df()
    con.close()
    return result


def enrich_with_probabilities(gold_rows: pd.DataFrame) -> pd.DataFrame:
    """`gold_rows` (SIM gold rows in memory: id_anriss, mu, xsi, tau0, h, d
    needed) with lambda_Ereignis and its five factors added, by the same join
    as the IFK curves (_events_sql) -- pgr-atlas's single-anriss "Rohdaten"
    export.

    Exhaustive, unlike the kachel-scoped joins: a row without a
    probability_lookup entry or an ablauf combination raises instead of
    dropping out, and so does an impossible lambda_ereignis (see
    _assert_valid_lambda_ereignis). No expert-mode overrides."""
    probability_columns = ['lambda_Hangmuren', 'p_raeumlich', 'p_A', 'p_h', 'p_ablauf', 'lambda_ereignis']
    if gold_rows.empty:
        return gold_rows.assign(**{column: pd.Series(dtype='float64') for column in probability_columns})

    con = _s3_gold_connection()
    _register_ablauf(con)
    con.register('gold_rows', gold_rows)
    _bind_probability_lookup(con, "SELECT DISTINCT id_anriss FROM gold_rows")

    missing_anriss = con.execute("""
        SELECT DISTINCT id_anriss
        FROM gold_rows
        ANTI JOIN probability_lookup USING (id_anriss)
    """).df()['id_anriss'].tolist()
    if missing_anriss:
        raise ValueError(f"enrich_with_probabilities: {len(missing_anriss)} id_anriss not found in "
                          f"probability_lookup: {missing_anriss}")
    missing_ablauf = con.execute("""
        SELECT DISTINCT mu, xsi, tau0
        FROM gold_rows
        ANTI JOIN ablauf USING (mu, xsi, tau0)
    """).df()
    if not missing_ablauf.empty:
        raise ValueError(f"enrich_with_probabilities: {len(missing_ablauf)} (mu, xsi, tau0) combo(s) not "
                          f"found in ablauf: {missing_ablauf.to_dict('records')}")

    result = con.execute(_events_sql("gold_rows")).df()
    con.close()
    # The rules of _assert_valid_lambda_ereignis, on the DataFrame.
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
    inside_polygon = ""
    if selection['type'] == 'polygon':
        inside_polygon = f"AND ST_Contains(ST_GeomFromText('{_ring_wkt(selection['ring'])}'), ST_Point(x, y))"
    try:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE gold_tile AS
            SELECT *
            FROM read_parquet('{kachel_s3_file(id_kachel)}')
            WHERE x >= {xmin} AND x <= {xmax}
              AND y >= {ymin} AND y <= {ymax}
              {inside_polygon}
        """)
    except duckdb.HTTPException as error:
        if _is_missing_kachel(error):
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
    return round(threshold / _TO_DISPLAY_UNIT[variable], 6)


def _curve_sql(variable: str) -> str:
    """SELECT the exceedance curve of one variable from `events`: per pixel and
    stored intensity (gold's integer unit), exceedance_rate = the summed
    lambda_ereignis of every event at that pixel with at least that intensity."""
    return f"""
        SELECT '{variable}' AS variable, x, y, intensity,
               SUM(rate) OVER (PARTITION BY x, y ORDER BY intensity DESC
                               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exceedance_rate
        FROM (
            SELECT x, y, "{_GOLD_COLUMN[variable]}" AS intensity, SUM(lambda_ereignis) AS rate
            FROM events
            GROUP BY x, y, "{_GOLD_COLUMN[variable]}"
        )"""


def _curves_insert_sql() -> str:
    """Appends the current kachel's exceedance curves (from `events`, built
    and checked by build_exceedance_curves) to `curves`, one _curve_sql per
    variable. The exceedance rate falls as the intensity rises, so Mode A is
    1 / rate at the smallest intensity >= the threshold, Mode B the largest
    intensity whose rate reaches 1 / return period -- one curve answers every
    threshold and return period of all three variables."""
    return "INSERT INTO curves" + "\n        UNION ALL".join(_curve_sql(variable) for variable in INTENSITY_VARS)


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
    exceedance curves (see _curve_sql) to the parquet out_path, which
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
    unfinished_path = f"{out_path}.{uuid.uuid4().hex}.tmp"
    with _JobDir() as job_dir:
        con = _job_connection(job_dir)
        try:
            con.execute("INSTALL httpfs; LOAD httpfs;")
            configure_s3_for_duckdb(con)
            if selection['type'] == 'polygon':
                # Only for an actual polygon clip (ST_Contains, _bind_gold_tile).
                con.execute("INSTALL spatial; LOAD spatial;")
            _register_ablauf(con, ablauf_override)
            con.execute("""
                CREATE TABLE curves (
                    variable VARCHAR, x DOUBLE, y DOUBLE, intensity INTEGER, exceedance_rate DOUBLE)
            """)
            insert_sql = _curves_insert_sql()
            for done, id_kachel in enumerate(kacheln):
                if progress_callback:
                    progress_callback(done, len(kacheln), f"Kachel {done + 1}/{len(kacheln)}")
                if not _bind_gold_tile(con, id_kachel, selection):
                    continue
                # Materialised once: lambda_ereignis feeds three curves and the
                # validity check below. Only the columns those need: a dense
                # kachel has 100M+ rows.
                con.execute(f"""
                    CREATE OR REPLACE TEMP TABLE events AS
                    SELECT x, y, "Fliesstiefe", "Fliessgeschwindigkeit", "Druck",
                           lambda_Hangmuren, lambda_ereignis
                    FROM ({_events_sql("gold_tile", p_h_mean, p_h_max)})
                """)
                _assert_valid_lambda_ereignis(con, "SELECT lambda_ereignis, lambda_Hangmuren FROM events",
                                              f"kachel {id_kachel}", allow_zero=has_overrides)
                con.execute(insert_sql)
            if progress_callback and kacheln:
                progress_callback(len(kacheln), len(kacheln), "Schreibe Kurven…")
            # Sorted by variable: raster_from_curves reads one variable at a
            # time and skips the other row groups.
            con.execute(f"""
                COPY (SELECT * FROM curves ORDER BY variable)
                TO '{unfinished_path}' (FORMAT parquet, COMPRESSION zstd)
            """)
            os.replace(unfinished_path, out_path)
        finally:
            con.close()
            if os.path.exists(unfinished_path):
                os.remove(unfinished_path)


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
                # The rate falls as the intensity rises: the largest rate at or
                # above the threshold is the rate of the smallest such intensity.
                pixels = con.execute(f"""
                    SELECT x, y, 1.0 / MAX(exceedance_rate) AS return_period
                    FROM {curves}
                    WHERE variable = ? AND intensity >= ?
                    GROUP BY x, y
                """, [variable, _raw_threshold(variable, threshold)]).df()
                if pixels.empty:
                    print("  No pixels exceed threshold — skipping.")
                    return []
                out_path = os.path.join(
                    out_dir, f'{variable}_at_{threshold}{INTENSITY_SUFFIX[variable]}_{bbox_slug}.tif')
                _write_single_band(pixels, 'return_period', out_path, {
                    'mode': 'A', 'variable': variable,
                    'threshold': str(threshold), 'threshold_units': INTENSITY_UNITS[variable],
                    'value_units': 'years (return period)', **tags,
                })
                return [out_path]
            min_exceedance_rate = 1.0 / return_period
            return_period_text = (str(int(return_period)) if float(return_period) == int(return_period)
                                  else str(return_period))
            out_paths = []
            for intensity_variable in INTENSITY_VARS:
                # The largest intensity still exceeded at least once per return period.
                pixels = con.execute(f"""
                    SELECT x, y, MAX(intensity) * {_TO_DISPLAY_UNIT[intensity_variable]!r} AS intensity
                    FROM {curves}
                    WHERE variable = ? AND exceedance_rate >= ?
                    GROUP BY x, y
                """, [intensity_variable, min_exceedance_rate]).df()
                if pixels.empty:
                    print(f"  No pixels reach T={return_period} yr for {intensity_variable} — skipping.")
                    continue
                out_path = os.path.join(out_dir, f'{intensity_variable}_rp{return_period_text}_{bbox_slug}.tif')
                _write_single_band(pixels, 'intensity', out_path, {
                    'mode': 'B', 'variable': intensity_variable,
                    'value_units': INTENSITY_UNITS[intensity_variable],
                    'return_period': return_period_text,
                    'exceedance_probability': f'{min_exceedance_rate:.2e}', **tags,
                })
                out_paths.append(out_path)
            return out_paths
        finally:
            con.close()


# Kacheln per request: 25 MB per kachel as a pessimistic worst case, against a
# quarter of the process's memory, and never more than 300. (2026-08-16: a
# 42x31 km bbox, ~1300 kacheln, grew the app to ~14 GB and took the whole WSL
# VM down.) check_raster_size rejects a larger selection before anything is read.
_RASTER_KACHEL_MEMORY_BUDGET_BYTES = 25 * 1024 * 1024
_RASTER_MAX_MEMORY_FRACTION = 0.25
_RASTER_MAX_KACHELN_CEILING = 300


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
        east_km, north_km = divmod(id_kachel, 10000)
        share = (max(0.0, min(xmax, (east_km + 1) * 1000) - max(xmin, east_km * 1000))
                 * max(0.0, min(ymax, (north_km + 1) * 1000) - max(ymin, north_km * 1000)) / 1e6)
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
# 2: columns i, p renamed to intensity, exceedance_rate.
_CURVES_FORMAT = 2


# Rough seconds per kachel, ~p10 and ~p90 of a 40-kachel benchmark on an
# 8-vCPU node in the bucket's data centre (2026-08-17): ~1 s for an empty
# kachel, up to ~400 s for the densest (364M rows). Density varies 1000x
# across the lake, hence a range and not one number.
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

    Expert-mode overrides (pgr-atlas's "Experten-Modus"), default None
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
