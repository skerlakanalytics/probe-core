"""IFK of an area: a simulation counts once, with its largest intensity over
the area's pixels (the SQL of build_area_event_maxima and compute_ifk_for_area,
on hand-made gold kacheln, without S3)."""

import duckdb
import pandas as pd
import pytest

from probe_core.data_lake.data_lake_schema import GOLD_SCHEMA_SIM_DUCKDB
from probe_core.derivate import maxi_ifk_and_raster as m

# Three neighbouring pixels of kachel 26001200, and one across its eastern
# border in kachel 26011200.
WEST, MIDDLE, EAST = (2600987.5, 1200002.5), (2600992.5, 1200002.5), (2600997.5, 1200002.5)
ACROSS = (2601002.5, 1200002.5)
NORTH = (2600992.5, 1200007.5)

# (id_anriss, h, pixel, depth cm, velocity cm/s, pressure kPa). Anriss 1 has two
# simulations (h = d = 100 and h = 140); every other column is the same for all.
GOLD_ROWS = [
    (1, 100, WEST, 10, 300, 5),     # simulation A: deepest in the west,
    (1, 100, MIDDLE, 30, 100, 9),   # ... fastest in the west, deepest in the middle
    (1, 100, EAST, 20, 200, 7),
    (1, 100, ACROSS, 50, 50, 2),    # ... and it crosses into the next kachel
    (1, 140, MIDDLE, 40, 400, 12),  # simulation B: one pixel
    (2, 100, EAST, 15, 150, 3),     # simulation C: one pixel
    (2, 100, NORTH, 99, 999, 99),   # ... and one north of the row
]
# lambda_ereignis per simulation: what probability_lookup x ablauf below give.
RATE = {"A": 0.001, "B": 0.002, "C": 0.004}


def _rect(xmin, ymin, xmax, ymax):
    return {"type": "bbox", "ring": m.rect_ring(xmin, ymin, xmax, ymax)}


ROW = _rect(2600985, 1200000, 2601005, 1200005)  # WEST, MIDDLE, EAST, ACROSS


@pytest.fixture()
def gold(tmp_path):
    """The gold rows as one parquet per kachel; returns id_kachel -> path."""
    con = duckdb.connect()
    columns_sql = ", ".join(f'"{name}" {sql_type}' for name, sql_type in GOLD_SCHEMA_SIM_DUCKDB.items())
    con.execute(f"CREATE TABLE gold ({columns_sql})")
    for id_anriss, h, (x, y), depth, velocity, pressure in GOLD_ROWS:
        con.execute("INSERT INTO gold VALUES (7, ?, 2600500, 1200500, 100, 100, ?, 0.05, 100, 1000, ?, ?, ?, ?, ?)",
                    [id_anriss, h, x, y, depth, velocity, pressure])
    files = {}
    for id_kachel in (26001200, 26011200):
        files[id_kachel] = str(tmp_path / f"{id_kachel}.parquet")
        con.execute(f"""
            COPY (SELECT * FROM gold WHERE floor(x / 1000)::INT * 10000 + floor(y / 1000)::INT = {id_kachel})
            TO '{files[id_kachel]}' (FORMAT parquet)
        """)
    return files


def _maxima(gold, selection, tmp_path) -> pd.DataFrame:
    """build_area_event_maxima's two SQL steps on the local kacheln."""
    con = duckdb.connect()
    if selection["type"] == "polygon":
        con.execute("INSTALL spatial; LOAD spatial;")
    m._create_event_maxima_per_kachel(con)
    for id_kachel in m.kacheln_for_bbox(*m.selection_bounds(selection)):
        m._insert_kachel_event_maxima(con, gold[id_kachel], selection)
    path = str(tmp_path / "maxima.parquet")
    m._write_event_maxima(con, path)
    return duckdb.sql(f"""
        SELECT id_anriss, h, "Fliesstiefe", "Fliessgeschwindigkeit", "Druck", pixel_count
        FROM '{path}' ORDER BY id_anriss, h
    """).df()


def _curve(maxima: pd.DataFrame, variable: str) -> list[tuple]:
    """The curve of `variable` from the maxima, with RATE as lambda_ereignis."""
    con = duckdb.connect()
    rates = pd.DataFrame({"id_anriss": [1, 1, 2], "h": [100, 140, 100], "lambda_ereignis": list(RATE.values())})
    con.register("maxima", maxima)
    con.register("rates", rates)
    con.execute("CREATE TEMP TABLE ereignisse AS SELECT * FROM maxima JOIN rates USING (id_anriss, h)")
    curve = m._ifk_curve(con, variable)
    return [(row.intensity, pytest.approx(row.p_exceedance)) for row in curve.itertuples()]


def test_a_simulation_counts_once_with_its_largest_intensity_per_variable(gold, tmp_path):
    maxima = _maxima(gold, _rect(2600985, 1200000, 2601000, 1200005), tmp_path)  # WEST, MIDDLE, EAST
    # Simulation A: depth from MIDDLE, velocity from WEST, pressure from MIDDLE.
    assert maxima.values.tolist() == [[1, 100, 30, 300, 9, 3], [1, 140, 40, 400, 12, 1], [2, 100, 15, 150, 3, 1]]


def test_a_simulation_across_a_kachel_border_counts_once(gold, tmp_path):
    maxima = _maxima(gold, ROW, tmp_path)
    assert len(maxima) == 3
    assert maxima.values.tolist()[0] == [1, 100, 50, 300, 9, 4]  # depth from ACROSS, four pixels


def test_the_curve_sums_the_rates_of_the_simulations_reaching_each_intensity(gold, tmp_path):
    maxima = _maxima(gold, ROW, tmp_path)
    # Depth maxima: C 0.15 m, B 0.40 m, A 0.50 m.
    assert _curve(maxima, "depth") == [(0.15, 0.007), (0.40, 0.003), (0.50, 0.001)]
    # Velocity maxima: C 1.5 m/s, A 3 m/s, B 4 m/s.
    assert _curve(maxima, "velocity") == [(1.5, 0.007), (3.0, 0.003), (4.0, 0.002)]


def test_a_one_pixel_area_is_that_pixel(gold, tmp_path):
    maxima = _maxima(gold, _rect(2600990, 1200000, 2600995, 1200005), tmp_path)  # MIDDLE
    assert maxima.values.tolist() == [[1, 100, 30, 100, 9, 1], [1, 140, 40, 400, 12, 1]]


def test_the_area_curve_is_never_below_a_pixel_curve_inside_it(gold, tmp_path):
    area = dict(_curve(_maxima(gold, ROW, tmp_path), "depth"))
    pixel = _curve(_maxima(gold, _rect(2600990, 1200000, 2600995, 1200005), tmp_path), "depth")
    for intensity, rate in pixel:
        area_rate = max(r.expected for i, r in area.items() if i >= intensity)
        assert area_rate >= rate.expected


def test_a_polygon_takes_the_pixels_whose_centre_is_inside(gold, tmp_path):
    # A triangle over the row's western part and NORTH: it holds WEST, MIDDLE
    # and NORTH, while its rectangle also holds EAST.
    triangle = {"type": "polygon", "ring": [[2600985, 1200000], [2600996, 1200000], [2600991, 1200011],
                                            [2600985, 1200011], [2600985, 1200000]]}
    try:
        maxima = _maxima(gold, triangle, tmp_path)
    except duckdb.IOException:
        pytest.skip("the spatial extension can't be installed (no network)")
    # A: WEST and MIDDLE; B: MIDDLE; C: NORTH only (EAST is outside the triangle).
    assert maxima.values.tolist() == [[1, 100, 30, 300, 9, 2], [1, 140, 40, 400, 12, 1], [2, 100, 99, 999, 99, 1]]


def test_cache_key_names_the_selection_only():
    key = m.area_maxima_cache_key(ROW)
    assert key == m.area_maxima_cache_key(m.selection_bounds(ROW))  # a bbox tuple is the same selection
    assert key != m.area_maxima_cache_key(_rect(2600985, 1200000, 2601000, 1200005))


def test_no_simulation_gives_empty_curves_and_a_header_only_file(gold, tmp_path, monkeypatch):
    monkeypatch.setattr(m, "RASTER_TEMP_DIR", str(tmp_path / "tmp"))
    empty = str(tmp_path / "maxima.parquet")
    _maxima(gold, _rect(2600000, 1200500, 2600005, 1200505), tmp_path)  # no gold row there
    result = m.compute_ifk_for_area(empty)
    assert result["n_ereignisse"] == 0 and all(curve.empty for curve in result["curves"].values())
    out = str(tmp_path / "ereignisse.csv")
    assert m.write_area_ereignisse(empty, out, column_names={"depth": "Fliesstiefe max [m]"}) == 0
    header = pd.read_csv(out).columns.tolist()
    assert "Fliesstiefe max [m]" in header and "pixel_count" in header and len(header) == len(m.AREA_EREIGNISSE_COLUMNS)
