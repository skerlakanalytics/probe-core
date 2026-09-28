"""Gebäudeschatten (building footprints as release areas): the affected-mask
rules, the S3 layout, and the read layer behind the app's Gebäudeschatten view.

Written by ProBE_control_center: gold (workers/gebaeudeschatten_gold_worker.py),
the affected/building masks (derivate/build_gebaeudeschatten_{affected,building}_mask.py)
and the buildings layer (derivate/build_gebaeudeschatten_buildings_layer.py).

The rules are defined once, here: the pipeline builds the canton-wide
affected-mask rasters with `affected_pixels_sql`, and the app explains single
pixels and buildings with `exclusion_reason_sql`, so a pixel the map shows as
affected is exactly one the explanation calls counting.

Rules, per gold row (one simulation of one building at one pixel):

  raw                            counts if Fliesstiefe >= MIN_DEPTH_CM, judged
                                 on SIM_SPATIAL's unzeroed depth
  shadowing_building             excludes the row's own building
                                 (own_building_mask = 1); the neighbour ring
                                 (= 2) counts by presence alone, because
                                 SIM_SPATIAL_ZEROED zeroed its depth; other
                                 rows need Fliesstiefe >= MIN_DEPTH_CM
  shadowing_building_surrounding excludes own building and neighbour ring,
                                 rows below MIN_DEPTH_CM, and the 8 neighbours
                                 of the building's own = 1 pixels. The last
                                 one only matters for buildings too small to
                                 contain a grid center: their single = 1 pixel
                                 is AvaFrame's release-search fallback and has
                                 no = 2 ring around it.

A pixel is affected when at least one row there counts; rows are judged per
building, so a pixel excluded for one building can still be affected by
another. own_building_mask itself is computed in gold (see the gold worker).
"""

import threading

import duckdb
import pandas as pd

from probe_core.data_lake.data_interface import S3_BUCKET_GOLD
from probe_core.data_lake.data_lake_schema import (
    DATA_LAKE_DIR_GEBAEUDESCHATTEN_DERIVATE,
    DATA_LAKE_DIR_GEBAEUDESCHATTEN_DERIVATE_AFFECTED_MASK,
    DATA_LAKE_DIR_GEBAEUDESCHATTEN_DERIVATE_BUILDING_MASK,
    GEBAEUDESCHATTEN_AFFECTED_MASK_MIN_DEPTH_CM,
    GEBAEUDESCHATTEN_AFFECTED_MASK_VARIANTS,
    GEBAEUDESCHATTEN_GOLD_REACH_VARIANTS_M,
    gebaeudeschatten_gold_sim_anriss_dir,
    gebaeudeschatten_gold_sim_spatial_dir,
    gebaeudeschatten_gold_sim_spatial_zeroed_dir,
)
from probe_core.resources import safe_duckdb_memory_limit
from probe_core.s3 import configure_s3_for_duckdb, s3_key_exists

VARIANTS = GEBAEUDESCHATTEN_AFFECTED_MASK_VARIANTS
REACHES_M = GEBAEUDESCHATTEN_GOLD_REACH_VARIANTS_M
MIN_DEPTH_CM = GEBAEUDESCHATTEN_AFFECTED_MASK_MIN_DEPTH_CM
RESOLUTION_M = 5.0
# Upper bound on a release polygon's bbox side (full-canton manifest
# 2026-09-21: max 374 m). A building reaching a pixel has its own pixels
# within reach + this of it -- see building_footprint_pixels.
MAX_BUILDING_EXTENT_M = 400

# 8-connected neighbours on the 5 m grid; the gold worker's
# OWN_BUILDING_MASK_NEIGHBOR_OFFSETS uses the same ones for own_building_mask = 2.
NEIGHBOR_OFFSETS = ((5, 0), (-5, 0), (0, 5), (0, -5), (5, 5), (5, -5), (-5, 5), (-5, -5))

# Exclusion reasons returned by exclusion_reason_sql (NULL = the row counts).
REASON_OWN_BUILDING = "own_building"          # own_building_mask = 1
REASON_OWN_NEIGHBOR = "own_neighbor"          # own_building_mask = 2
REASON_BELOW_MIN_DEPTH = "below_min_depth"    # Fliesstiefe < MIN_DEPTH_CM
REASON_FALLBACK_NEIGHBOR = "fallback_neighbor"  # next to the building's own = 1 pixel, no = 2 there
REASONS = (REASON_OWN_BUILDING, REASON_OWN_NEIGHBOR, REASON_BELOW_MIN_DEPTH, REASON_FALLBACK_NEIGHBOR)

# Buildings layer (id_start -> footprint), built from the ingested manifest.
# The PMTiles sits at the bucket root: the app's tile proxy serves root-level
# *.pmtiles keys only.
BUILDINGS_PMTILES_KEY = "gebaeudeschatten_buildings.pmtiles"
BUILDINGS_PMTILES_LAYER = "buildings"
BUILDINGS_PARQUET_KEY = f"{DATA_LAKE_DIR_GEBAEUDESCHATTEN_DERIVATE}/buildings/buildings.parquet"
BUILDING_MASK_KEY = f"{DATA_LAKE_DIR_GEBAEUDESCHATTEN_DERIVATE_BUILDING_MASK}/canton_building_mask.tif"

_SIM_KEY_COLS = ("id_start", "A", "h", "mu", "xsi", "tau0")


def _check(reach_m: int, variant: str | None = None) -> None:
    if reach_m not in REACHES_M:
        raise ValueError(f"reach_m={reach_m!r} not in {REACHES_M}")
    if variant is not None and variant not in VARIANTS:
        raise ValueError(f"variant={variant!r} not in {VARIANTS}")


# ── paths ────────────────────────────────────────────────────────────────────

def affected_mask_filename(reach_m: int, variant: str) -> str:
    _check(reach_m, variant)
    return f"canton_affected_mask_{variant}_{reach_m}m.tif"


def affected_mask_key(reach_m: int, variant: str) -> str:
    return f"{DATA_LAKE_DIR_GEBAEUDESCHATTEN_DERIVATE_AFFECTED_MASK}/{affected_mask_filename(reach_m, variant)}"


def gold_key(reach_m: int, dataset: str) -> str:
    """dataset: 'SIM_SPATIAL' | 'SIM_SPATIAL_ZEROED' | 'SIM_ANRISS'."""
    _check(reach_m)
    if dataset == "SIM_SPATIAL":
        return f"{gebaeudeschatten_gold_sim_spatial_dir(reach_m)}/data.parquet"
    if dataset == "SIM_SPATIAL_ZEROED":
        return f"{gebaeudeschatten_gold_sim_spatial_zeroed_dir(reach_m)}/data.parquet"
    if dataset == "SIM_ANRISS":
        return f"{gebaeudeschatten_gold_sim_anriss_dir(reach_m)}/sim_anriss.parquet"
    raise ValueError(f"unknown gold dataset {dataset!r}")


def affected_mask_source_key(reach_m: int, variant: str) -> str:
    """The gold file a variant's raster is built from: raw judges SIM_SPATIAL's
    unzeroed depth, the other two need own_building_mask from SIM_SPATIAL_ZEROED."""
    _check(reach_m, variant)
    return gold_key(reach_m, "SIM_SPATIAL" if variant == "raw" else "SIM_SPATIAL_ZEROED")


# ── rules ────────────────────────────────────────────────────────────────────

def exclusion_reason_sql(variant: str, depth: str = "Fliesstiefe", mask: str = "own_building_mask",
                         in_own1_ring: str = "in_own1_ring") -> str:
    """SQL CASE expression: NULL if the row counts as affected under `variant`,
    else one of REASONS. `depth` is Fliesstiefe [cm]; for raw it must be the
    unzeroed value (SIM_SPATIAL), for the other two either works (they only
    judge depth where own_building_mask = 0, and zeroing leaves those rows
    alone). `in_own1_ring` (surrounding only) is a boolean column from
    own1_ring_sql."""
    if variant not in VARIANTS:
        raise ValueError(f"variant={variant!r} not in {VARIANTS}")
    below = f"WHEN {depth} < {MIN_DEPTH_CM} THEN '{REASON_BELOW_MIN_DEPTH}'"
    if variant == "raw":
        return f"(CASE {below} END)"
    if variant == "shadowing_building":
        return (f"(CASE WHEN {mask} = 1 THEN '{REASON_OWN_BUILDING}' "
                f"WHEN {mask} = 0 AND {depth} < {MIN_DEPTH_CM} THEN '{REASON_BELOW_MIN_DEPTH}' END)")
    return (f"(CASE WHEN {mask} = 1 THEN '{REASON_OWN_BUILDING}' "
            f"WHEN {mask} = 2 THEN '{REASON_OWN_NEIGHBOR}' "
            f"{below} "
            f"WHEN {in_own1_ring} THEN '{REASON_FALLBACK_NEIGHBOR}' END)")


def own1_ring_sql(own1_sql: str) -> str:
    """SELECT (id_start, x, y) of the 8 grid neighbours of every own = 1 pixel
    in `own1_sql` (a query yielding id_start, x, y). An equi-join target, not
    a range join: at canton scale that is the difference between finishing
    and not."""
    offsets = ", ".join(f"({dx}, {dy})" for dx, dy in NEIGHBOR_OFFSETS)
    return (f"SELECT DISTINCT o.id_start, o.x + off.dx AS x, o.y + off.dy AS y "
            f"FROM ({own1_sql}) o CROSS JOIN (VALUES {offsets}) AS off(dx, dy)")


def affected_pixels_sql(variant: str, source: str) -> str:
    """The canton-wide raster query: DISTINCT (x, y) with at least one counting
    row. `source` is the parquet path/URL of affected_mask_source_key's file."""
    if variant not in VARIANTS:
        raise ValueError(f"variant={variant!r} not in {VARIANTS}")
    reason = exclusion_reason_sql(variant)
    if variant != "shadowing_building_surrounding":
        return f"SELECT DISTINCT x, y FROM read_parquet('{source}') WHERE {reason} IS NULL"
    return f"""
        WITH base AS (
            SELECT id_start, x, y, own_building_mask, Fliesstiefe FROM read_parquet('{source}')
        ),
        ring AS ({own1_ring_sql("SELECT id_start, x, y FROM base WHERE own_building_mask = 1")}),
        flagged AS (
            SELECT b.*, r.id_start IS NOT NULL AS in_own1_ring
            FROM base b LEFT JOIN ring r USING (id_start, x, y)
        )
        SELECT DISTINCT x, y FROM flagged WHERE {reason} IS NULL
    """


def _reason_columns_sql() -> str:
    return ", ".join(f"{exclusion_reason_sql(v)} AS reason_{v}" for v in VARIANTS)


# ── read layer (app) ─────────────────────────────────────────────────────────

_base_con = None
_base_con_lock = threading.Lock()


def _cursor() -> duckdb.DuckDBPyConnection:
    """A cursor on one process-wide database. Sharing it keeps DuckDB's
    object cache (parquet footers) warm: the first read of a gold file costs
    ~1.2 s for its metadata, later reads ~0.1 s. Cursors are per call, so
    threads don't share one; S3 settings are per session, so each cursor
    gets them."""
    global _base_con
    with _base_con_lock:
        if _base_con is None:
            con = duckdb.connect()
            con.execute("INSTALL httpfs; LOAD httpfs;")
            con.execute(f"SET memory_limit='{safe_duckdb_memory_limit()}'")
            con.execute("SET enable_object_cache=true")
            _base_con = con
        cur = _base_con.cursor()
    cur.execute("SET http_retries=4")
    configure_s3_for_duckdb(cur)
    return cur


# Values go into the SQL as literals, not prepared-statement parameters: with
# parameters DuckDB does not prune row groups by their min/max, and a point
# lookup in the x/y-sorted gold takes 4.6 s instead of 0.1 s (measured
# 2026-09-28). Every value passes through float()/int() first.
def _f(v) -> str:
    return repr(float(v))


def _ids(id_starts) -> str:
    return ", ".join(str(int(i)) for i in id_starts)


def _url(bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key}"


def is_available(bucket: str = S3_BUCKET_GOLD) -> bool:
    """Whether `bucket` holds what the Gebäudeschatten view needs."""
    return (s3_key_exists(bucket, BUILDINGS_PMTILES_KEY)
            and s3_key_exists(bucket, gold_key(max(REACHES_M), "SIM_SPATIAL_ZEROED")))


def building_rows(id_start: int, reach_m: int, bucket: str = S3_BUCKET_GOLD) -> pd.DataFrame:
    """Every gold row of one building (all its simulations), unzeroed values
    plus own_building_mask, in_own1_ring and reason_<variant> for each variant
    (NULL = counts). Empty if the building has no rows at this reach."""
    _check(reach_m)
    con = _cursor()
    try:
        # SIM_ANRISS is sorted by id_start, so this reads ~one row group; its
        # bbox then prunes the x/y-sorted SIM_SPATIAL_ZEROED to a few more.
        con.execute(f"""
            CREATE TEMP TABLE s AS
            SELECT id_start, A, h, mu, xsi, tau0, x, y, Fliesstiefe, Fliessgeschwindigkeit, Druck
            FROM read_parquet('{_url(bucket, gold_key(reach_m, "SIM_ANRISS"))}') WHERE id_start = {int(id_start)}
        """)
        bounds = con.execute("SELECT MIN(x), MAX(x), MIN(y), MAX(y) FROM s").fetchone()
        if bounds[0] is None:
            bounds = (0, -1, 0, -1)  # empty range: no rows, same columns
        return con.execute(f"""
            WITH z AS (
                SELECT {", ".join(_SIM_KEY_COLS)}, x, y, own_building_mask
                FROM read_parquet('{_url(bucket, gold_key(reach_m, "SIM_SPATIAL_ZEROED"))}')
                WHERE id_start = {int(id_start)}
                  AND x BETWEEN {_f(bounds[0])} AND {_f(bounds[1])} AND y BETWEEN {_f(bounds[2])} AND {_f(bounds[3])}
            ),
            rows AS (SELECT s.*, z.own_building_mask FROM s JOIN z USING ({", ".join(_SIM_KEY_COLS)}, x, y)),
            ring AS ({own1_ring_sql("SELECT id_start, x, y FROM rows WHERE own_building_mask = 1")}),
            flagged AS (
                SELECT r.*, ring.id_start IS NOT NULL AS in_own1_ring
                FROM rows r LEFT JOIN ring USING (id_start, x, y)
            )
            SELECT *, {_reason_columns_sql()} FROM flagged
            ORDER BY A, h, mu, xsi, tau0, x, y
        """).df()
    finally:
        con.close()


def pixel_rows(x: float, y: float, reach_m: int, bucket: str = S3_BUCKET_GOLD) -> pd.DataFrame:
    """Every gold row at one pixel center (every simulation of every building
    that reaches it), unzeroed values plus own_building_mask, in_own1_ring and
    reason_<variant> for each variant (NULL = counts)."""
    _check(reach_m)
    zeroed = _url(bucket, gold_key(reach_m, "SIM_SPATIAL_ZEROED"))
    spatial = _url(bucket, gold_key(reach_m, "SIM_SPATIAL"))
    con = _cursor()
    try:
        # The fallback ring test only needs own = 1 pixels within one cell of
        # (x, y) -- both files are x/y-sorted, so every read here is pruned.
        return con.execute(f"""
            WITH s AS (
                SELECT {", ".join(_SIM_KEY_COLS)}, x, y, Fliesstiefe, Fliessgeschwindigkeit, Druck
                FROM read_parquet('{spatial}') WHERE x = {_f(x)} AND y = {_f(y)}
            ),
            z AS (
                SELECT {", ".join(_SIM_KEY_COLS)}, x, y, own_building_mask
                FROM read_parquet('{zeroed}') WHERE x = {_f(x)} AND y = {_f(y)}
            ),
            near_own1 AS (
                SELECT id_start, x, y FROM read_parquet('{zeroed}')
                WHERE x BETWEEN {_f(x - RESOLUTION_M)} AND {_f(x + RESOLUTION_M)}
                  AND y BETWEEN {_f(y - RESOLUTION_M)} AND {_f(y + RESOLUTION_M)} AND own_building_mask = 1
            ),
            ring AS ({own1_ring_sql("SELECT * FROM near_own1")}),
            rows AS (SELECT s.*, z.own_building_mask FROM s JOIN z USING ({", ".join(_SIM_KEY_COLS)}, x, y)),
            flagged AS (
                SELECT r.*, ring.id_start IS NOT NULL AS in_own1_ring
                FROM rows r LEFT JOIN ring USING (id_start, x, y)
            )
            SELECT *, {_reason_columns_sql()} FROM flagged
            ORDER BY id_start, A, h, mu, xsi, tau0
        """).df()
    finally:
        con.close()


def building_footprint_pixels(id_starts: list[int], reach_m: int, near: tuple[float, float, float, float],
                              bucket: str = S3_BUCKET_GOLD) -> pd.DataFrame:
    """DISTINCT (id_start, x, y, own_building_mask) of the own = 1 and ring = 2
    pixels of several buildings. `near` (xmin, ymin, xmax, ymax, LV95) must
    contain them; it prunes the x/y-sorted file (for the buildings reaching one
    pixel, that pixel padded by reach_m + MAX_BUILDING_EXTENT_M is enough)."""
    _check(reach_m)
    if not id_starts:
        return pd.DataFrame(columns=["id_start", "x", "y", "own_building_mask"])
    con = _cursor()
    try:
        return con.execute(f"""
            SELECT DISTINCT id_start, x, y, own_building_mask
            FROM read_parquet('{_url(bucket, gold_key(reach_m, "SIM_SPATIAL_ZEROED"))}')
            WHERE own_building_mask IN (1, 2) AND id_start IN ({_ids(id_starts)})
              AND x BETWEEN {_f(near[0])} AND {_f(near[2])} AND y BETWEEN {_f(near[1])} AND {_f(near[3])}
            ORDER BY id_start, own_building_mask, x, y
        """).df()
    finally:
        con.close()


def building_geometries(id_starts: list[int], bucket: str = S3_BUCKET_GOLD) -> pd.DataFrame:
    """id_start, geometry_wkb (LV95 footprint, courtyard holes filled -- the
    polygon that was simulated) for the given buildings."""
    if not id_starts:
        return pd.DataFrame(columns=["id_start", "geometry_wkb"])
    con = _cursor()
    try:
        return con.execute(f"""
            SELECT id_start, geometry_wkb FROM read_parquet('{_url(bucket, BUILDINGS_PARQUET_KEY)}')
            WHERE id_start IN ({_ids(id_starts)})
            ORDER BY id_start
        """).df()
    finally:
        con.close()
