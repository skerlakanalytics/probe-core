"""Gebäudeschatten affected-mask rules: reason codes per row, and the raster
query must select exactly what ProBE_control_center's
derivate/build_gebaeudeschatten_affected_mask.py selected before the rules
moved here (its SQL as of 2026-09-28 is the reference below)."""

import duckdb
import pandas as pd
import pytest

from probe_core import gebaeudeschatten as g

MIN = 1  # the minimum the pipeline used before 10 cm was added

# (id_start, x, y, own_building_mask, unzeroed Fliesstiefe [cm])
# Building 1 is ordinary: own pixel, ring pixel, one deep and one shallow runout pixel.
# Building 2 is too small for a grid center: its single own = 1 pixel is the
# AvaFrame fallback, with no = 2 ring, and it also reaches building 1's own pixel.
ROWS = [
    (1, 102.5, 102.5, 1, 40),
    (1, 107.5, 102.5, 2, 40),
    (1, 112.5, 102.5, 0, 5),
    (1, 117.5, 102.5, 0, 0),
    (2, 202.5, 202.5, 1, 40),
    (2, 207.5, 202.5, 0, 30),
    (2, 212.5, 202.5, 0, 30),
    (2, 102.5, 102.5, 0, 10),
]

EXPECTED_REASONS = {  # (id_start, x) -> (raw, shadowing_building, surrounding)
    (1, 102.5): (None, g.REASON_OWN_BUILDING, g.REASON_OWN_BUILDING),
    (1, 107.5): (None, None, g.REASON_OWN_NEIGHBOR),
    (1, 112.5): (None, None, None),
    (1, 117.5): (g.REASON_BELOW_MIN_DEPTH,) * 3,
    (2, 202.5): (None, g.REASON_OWN_BUILDING, g.REASON_OWN_BUILDING),
    (2, 207.5): (None, None, g.REASON_FALLBACK_NEIGHBOR),
    (2, 212.5): (None, None, None),
    (2, 102.5): (None, None, None),
}
# At 10 cm only building 1's shallow runout pixel (5 cm) changes; building 2's 10 cm row still counts.
EXPECTED_REASONS_10CM = {**EXPECTED_REASONS, (1, 112.5): (g.REASON_BELOW_MIN_DEPTH,) * 3}

# The pipeline's queries before the move (derivate/build_gebaeudeschatten_affected_mask.py).
_OLD_MASK_FILTER = {
    "raw": "WHERE Fliesstiefe >= {min}",
    "shadowing_building": "WHERE own_building_mask != 1 AND (own_building_mask != 0 OR Fliesstiefe >= {min})",
}
_OFFSETS = ", ".join(f"({dx}, {dy})" for dx, dy in g.NEIGHBOR_OFFSETS)
_OLD_SURROUNDING_SQL = f"""
    WITH base AS (SELECT id_start, x, y, own_building_mask, Fliesstiefe FROM read_parquet('{{sim_file}}')),
    own1_ring AS (
        SELECT DISTINCT o.id_start, o.x + off.dx AS x, o.y + off.dy AS y
        FROM (SELECT DISTINCT id_start, x, y FROM base WHERE own_building_mask = 1) o
        CROSS JOIN (VALUES {_OFFSETS}) AS off(dx, dy)
    )
    SELECT DISTINCT b.x, b.y FROM base b
    WHERE b.own_building_mask = 0 AND b.Fliesstiefe >= {{min}}
      AND NOT EXISTS (SELECT 1 FROM own1_ring r WHERE r.id_start = b.id_start AND r.x = b.x AND r.y = b.y)
"""


@pytest.fixture
def gold_files(tmp_path):
    """SIM_SPATIAL (unzeroed) and SIM_SPATIAL_ZEROED (depth 0 where mask is 1/2), gold column types."""
    df = pd.DataFrame(ROWS, columns=["id_start", "x", "y", "own_building_mask", "Fliesstiefe"])
    zeroed = df.assign(Fliesstiefe=df.Fliesstiefe.where(df.own_building_mask == 0, 0))
    con = duckdb.connect()
    paths = {}
    for name, frame in (("spatial", df.drop(columns="own_building_mask")), ("zeroed", zeroed)):
        con.register("frame", frame)
        paths[name] = str(tmp_path / f"{name}.parquet")
        mask_col = ", own_building_mask::TINYINT AS own_building_mask" if name == "zeroed" else ""
        con.execute(f"""COPY (SELECT id_start::BIGINT AS id_start, x::FLOAT AS x, y::FLOAT AS y,
                                     Fliesstiefe::INTEGER AS Fliesstiefe{mask_col} FROM frame)
                        TO '{paths[name]}' (FORMAT PARQUET)""")
        con.unregister("frame")
    return paths


def _pixels(sql):
    return set(duckdb.connect().execute(sql).fetchall())


def _source(gold_files, variant):
    return gold_files["spatial"] if variant == "raw" else gold_files["zeroed"]


@pytest.mark.parametrize("min_depth", g.MIN_DEPTHS_CM)
@pytest.mark.parametrize("variant", g.VARIANTS)
def test_affected_pixels_match_the_pipelines_previous_sql(gold_files, variant, min_depth):
    """The previous SQL with its fixed 1 cm replaced by each minimum."""
    src = _source(gold_files, variant)
    old = (_OLD_SURROUNDING_SQL.format(sim_file=src, min=min_depth) if variant == "shadowing_building_surrounding"
           else f"SELECT DISTINCT x, y FROM read_parquet('{src}') {_OLD_MASK_FILTER[variant].format(min=min_depth)}")
    new = _pixels(g.affected_pixels_sql(variant, src, min_depth))
    assert new == _pixels(old)
    assert new  # the fixture exercises every variant with a non-empty result


def _affected(gold_files, min_depth):
    return {v: {x for x, _ in _pixels(g.affected_pixels_sql(v, _source(gold_files, v), min_depth))}
            for v in g.VARIANTS}


def test_expected_affected_pixels(gold_files):
    got = _affected(gold_files, 1)
    assert got["raw"] == {102.5, 107.5, 112.5, 202.5, 207.5, 212.5}
    # 102.5 stays affected: excluded for building 1 (own), counts for building 2
    assert got["shadowing_building"] == {102.5, 107.5, 112.5, 207.5, 212.5}
    assert got["shadowing_building_surrounding"] == {102.5, 112.5, 212.5}


def test_expected_affected_pixels_10cm(gold_files):
    got = _affected(gold_files, 10)
    # 112.5 (5 cm) drops out everywhere; 102.5 stays via building 2's 10 cm row (>= is inclusive)
    assert got["raw"] == {102.5, 107.5, 202.5, 207.5, 212.5}
    assert got["shadowing_building"] == {102.5, 107.5, 207.5, 212.5}
    assert got["shadowing_building_surrounding"] == {102.5, 212.5}


@pytest.mark.parametrize("min_depth, expected", [(1, EXPECTED_REASONS), (10, EXPECTED_REASONS_10CM)])
def test_reason_per_row(gold_files, min_depth, expected):
    """What building_rows/pixel_rows compute: reasons on unzeroed depth + mask + ring."""
    sql = f"""
        WITH rows AS (
            SELECT s.id_start, s.x, s.y, s.Fliesstiefe, z.own_building_mask
            FROM read_parquet('{gold_files["spatial"]}') s
            JOIN read_parquet('{gold_files["zeroed"]}') z USING (id_start, x, y)
        ),
        ring AS ({g.own1_ring_sql("SELECT id_start, x, y FROM rows WHERE own_building_mask = 1")}),
        flagged AS (SELECT r.*, ring.id_start IS NOT NULL AS in_own1_ring
                    FROM rows r LEFT JOIN ring USING (id_start, x, y))
        SELECT id_start, x, {", ".join(g.exclusion_reason_sql(v, min_depth) for v in g.VARIANTS)} FROM flagged
    """
    got = {(i, x): tuple(r) for i, x, *r in duckdb.connect().execute(sql).fetchall()}
    assert got == expected


def test_paths():
    assert g.affected_mask_key(800, "raw", 1) == \
        "Gebaeudeschatten-Data-Lake-Derivate/affected_mask/canton_affected_mask_raw_800m_min1cm.tif"
    assert g.affected_mask_key(100, "shadowing_building", 10) == \
        "Gebaeudeschatten-Data-Lake-Derivate/affected_mask/canton_affected_mask_shadowing_building_100m_min10cm.tif"
    assert g.reason_column("raw", 10) == "reason_raw_min10cm"
    assert g.affected_mask_source_key(50, "raw") == \
        "Gebaeudeschatten-Data-Lake-Gold/REACH_50M/SIM_SPATIAL/data.parquet"
    assert g.affected_mask_source_key(50, "shadowing_building") == \
        "Gebaeudeschatten-Data-Lake-Gold/REACH_50M/SIM_SPATIAL_ZEROED/data.parquet"
    assert g.gold_key(100, "SIM_ANRISS") == "Gebaeudeschatten-Data-Lake-Gold/REACH_100M/SIM_ANRISS/sim_anriss.parquet"
    assert g.BUILDING_MASK_KEY == "Gebaeudeschatten-Data-Lake-Derivate/building_mask/canton_building_mask.tif"
    assert "/" not in g.BUILDINGS_PMTILES_KEY  # the app's tile proxy serves root-level keys only
    with pytest.raises(ValueError):
        g.affected_mask_key(200, "raw", 1)
    with pytest.raises(ValueError):
        g.affected_mask_key(800, "raw", 5)
    with pytest.raises(ValueError):
        g.exclusion_reason_sql("any_building", 1)


def test_literals_are_numbers_only():
    assert g._float_literal(2636392.5) == "2636392.5"
    assert g._id_list([3, 1]) == "3, 1"
    with pytest.raises(ValueError):
        g._float_literal("1; DROP TABLE x")
    with pytest.raises(ValueError):
        g._id_list(["1) OR (1=1"])
