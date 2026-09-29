"""Exceedance curves (build_exceedance_curves' SQL) and the rasters read off
them, on a hand-made pixel; the row estimate and the cache key, without S3."""

import duckdb
import pandas as pd
import pytest
import rasterio

from probe_core.derivate import maxi_ifk_and_raster as m

# One pixel, three events: depth 10 cm at rate 0.001, 20 cm at 0.002, 20 cm at 0.004.
PIXEL = (2600002.5, 1200002.5)
EVENTS = [(10, 0.001), (20, 0.002), (20, 0.004)]
SELECTION = {"type": "bbox", "ring": [[2600000, 1200000], [2600005, 1200000], [2600005, 1200005],
                                      [2600000, 1200005], [2600000, 1200000]]}


@pytest.fixture()
def curves(tmp_path):
    con = duckdb.connect()
    con.execute("""CREATE TABLE base (x DOUBLE, y DOUBLE, "Fliesstiefe" INTEGER,
                   "Fliessgeschwindigkeit" INTEGER, "Druck" INTEGER, lambda_ereignis DOUBLE)""")
    for depth, rate in EVENTS:
        con.execute("INSERT INTO base VALUES (?, ?, ?, ?, ?, ?)", [*PIXEL, depth, depth, depth, rate])
    con.execute("CREATE TABLE curves (variable VARCHAR, x DOUBLE, y DOUBLE, i INTEGER, p DOUBLE)")
    con.execute(m._curves_insert_sql())
    path = tmp_path / "curves.parquet"
    con.execute(f"COPY curves TO '{path}' (FORMAT parquet)")
    return str(path)


def test_curve_sums_the_rates_at_or_above_each_intensity(curves):
    rows = duckdb.sql(f"SELECT i, p FROM '{curves}' WHERE variable = 'depth' ORDER BY i").fetchall()
    assert rows == [(10, pytest.approx(0.007)), (20, pytest.approx(0.006))]


def _value(path):
    with rasterio.open(path) as r:
        return float(r.read(1)[0, 0])


@pytest.mark.parametrize("threshold, return_period", [(0.1, 1 / 0.007), (0.15, 1 / 0.006), (0.2, 1 / 0.006)])
def test_mode_a_is_the_return_period_at_the_threshold(curves, tmp_path, monkeypatch, threshold, return_period):
    monkeypatch.setattr(m, "RASTER_TEMP_DIR", str(tmp_path / "tmp"))
    (path,) = m.raster_from_curves(curves, SELECTION, "a", variable="depth", threshold=threshold,
                                   out_dir=str(tmp_path))
    assert _value(path) == pytest.approx(return_period, rel=1e-6)


def test_mode_a_above_every_event_writes_nothing(curves, tmp_path, monkeypatch):
    monkeypatch.setattr(m, "RASTER_TEMP_DIR", str(tmp_path / "tmp"))
    assert m.raster_from_curves(curves, SELECTION, "a", variable="depth", threshold=0.3,
                                out_dir=str(tmp_path)) == []


@pytest.mark.parametrize("return_period, depth_m", [(150, 0.1), (170, 0.2)])
def test_mode_b_is_the_intensity_at_the_return_period(curves, tmp_path, monkeypatch, return_period, depth_m):
    # 1/150 = 0.0067: only p(10 cm) = 0.007 reaches it; 1/170 = 0.0059: p(20 cm) = 0.006 does too.
    monkeypatch.setattr(m, "RASTER_TEMP_DIR", str(tmp_path / "tmp"))
    paths = m.raster_from_curves(curves, SELECTION, "b", return_period=return_period, out_dir=str(tmp_path))
    assert [p.rsplit("/", 1)[1].split("_")[0] for p in paths] == ["depth", "velocity", "pressure"]
    assert _value(paths[0]) == pytest.approx(depth_m)


def test_mode_b_below_every_rate_writes_nothing(curves, tmp_path, monkeypatch):
    monkeypatch.setattr(m, "RASTER_TEMP_DIR", str(tmp_path / "tmp"))
    assert m.raster_from_curves(curves, SELECTION, "b", return_period=100, out_dir=str(tmp_path)) == []


def test_row_estimate_scales_each_kachel_by_the_covered_share(monkeypatch):
    # A 500 x 1000 m box: half of kachel 26001200 (7.5 GB = 2000 M rows).
    monkeypatch.setattr(m, "gold_kachel_bytes", lambda: {26001200: 7_500_000_000})
    box = (2600000, 1200000, 2600500, 1201000)
    assert m.estimate_raster_rows(box) == pytest.approx(1_000_000_000, rel=1e-9)
    with pytest.raises(m.RasterBboxTooLargeError, match="Simulationspixel"):
        m.check_raster_size(box)
    monkeypatch.setenv("PROBE_RASTER_MAX_ROWS", "1200000000")
    m.check_raster_size(box)


class _FakeS3:
    etags = {"lookup": '"a"', "ablauf": '"b"'}

    def head_object(self, Bucket, Key):
        return {"ETag": self.etags["lookup" if Key == m.DATA_LAKE_PROBABILITIES_RATES_LOOKUP else "ablauf"]}


def test_cache_key_changes_with_area_probabilities_and_a_new_delivery(monkeypatch):
    s3 = _FakeS3()
    monkeypatch.setattr(m, "get_s3_client", lambda: s3)
    key = m.curves_cache_key(SELECTION)
    assert key == m.curves_cache_key(SELECTION)
    other_area = {"type": "bbox", "ring": [[x + 5, y] for x, y in SELECTION["ring"]]}
    ablauf = pd.DataFrame({"mu": [0.1], "xsi": [500], "tau0": [0], "p_Ablauf": [1.0]})
    variants = [m.curves_cache_key(other_area), m.curves_cache_key(SELECTION, p_h_mean=0.4, p_h_max=0.6),
                m.curves_cache_key(SELECTION, ablauf_override=ablauf)]
    s3.etags = {**s3.etags, "lookup": '"new delivery"'}
    variants.append(m.curves_cache_key(SELECTION))
    assert len({key, *variants}) == 5
