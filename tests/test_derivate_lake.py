"""Derivate lake layout: paths and band names must match what ProBE_control_center's
derivate/run_derivate.py writes (derivate/producers/exceedance.py column names)."""

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

from probe_core.derivate import derivate_lake as dl


def test_paths():
    assert dl.kachel_key("return_period", 25571214, "depth") == \
        "Data-Lake-Derivate/return_period/variable=depth/id_kachel=25571214/data.tif"
    assert dl.kachel_key("combined_hit_rate", 1, "depth01_pressure3") == \
        "Data-Lake-Derivate/combined_hit_rate/combo=depth01_pressure3/id_kachel=1/data.tif"
    assert dl.kachel_key("sim_count", 1) == "Data-Lake-Derivate/sim_count/id_kachel=1/data.tif"
    assert dl.kachel_key("start_rate", 1) == "Data-Lake-Derivate/start_rate/id_kachel=1/data.tif"
    assert dl.mosaic_key("intensity", "velocity") == "Data-Lake-Derivate/intensity/_mosaic/variable=velocity/canton.tif"
    assert dl.mosaic_stats_key("affected_mask") == "Data-Lake-Derivate/affected_mask/_mosaic/canton_bands.json"


def test_partition_required_or_forbidden():
    with pytest.raises(ValueError):
        dl.dataset_prefix("return_period")
    with pytest.raises(ValueError):
        dl.dataset_prefix("affected_mask", "depth")


@pytest.mark.parametrize("metric,name,expected", [
    ("return_period", "depth_0_25m_rp", {"threshold": 0.25}),
    ("return_period", "velocity_10ms_rp", {"threshold": 10.0}),
    ("return_period", "pressure_1000kpa_rp", {"threshold": 1000.0}),
    ("hit_rate", "depth_0_01m_hitrate", {"threshold": 0.01}),
    ("hit_rate", "velocity_0_1ms_hitrate", {"threshold": 0.1}),
    ("intensity", "depth_rp1000000y", {"return_period": 1000000}),
    ("combined_hit_rate", "depth01_pressure3_hitrate", {"stat": "rate"}),
    ("combined_hit_rate", "depth01_pressure3_hitrate80y", {"stat": "p80"}),
    ("sim_count", None, {}),
    ("start_rate", "h_0_5m_startrate", {"min_h": 0.5}),
    ("start_rate", "h_1m_startrate", {"min_h": 1.0}),
])
def test_parse_band(metric, name, expected):
    assert dl.parse_band(metric, name) == expected


def test_parse_band_rejects_foreign_names():
    with pytest.raises(ValueError):
        dl.parse_band("intensity", "depth_0_25m_rp")


def test_snapped_window_snaps_outward_on_lv95_grid(tmp_path):
    # 5 m grid with its edge on a round km: pixel centres at k*5+2.5
    path = tmp_path / "m.tif"
    with rasterio.open(path, "w", driver="GTiff", width=200, height=200, count=1, dtype="float32",
                       crs="EPSG:2056", transform=from_origin(2600000, 1200000, 5, 5)) as ds:
        ds.write(np.zeros((1, 200, 200), dtype="float32"))
    with rasterio.open(path) as ds:
        w = dl.snapped_window(ds, 2600003, 1199990, 2600011, 1199998)
        assert (w.col_off, w.row_off, w.width, w.height) == (0, 0, 3, 2)
        assert dl.snapped_window(ds, 2500000, 1100000, 2500010, 1100010).width == 0


def test_column_names_match_legacy_layout():
    # return_period/intensity names are the legacy raster/cfg_<hash>/ band names -- migrated and
    # freshly computed files must carry identical band descriptions
    assert dl.return_period_column("depth", 1.0) == "depth_1m_rp"
    assert dl.return_period_column("velocity", 1.0) == "velocity_1ms_rp"
    assert dl.return_period_column("pressure", 100.0) == "pressure_100kpa_rp"
    assert dl.hit_rate_column("depth", 0.25) == "depth_0_25m_hitrate"
    assert dl.intensity_column("depth", 1000000) == "depth_rp1000000y"
    assert dl.combined_hit_rate_columns("c") == ("c_hitrate", "c_hitrate80y")
    assert dl.fmt_num(0.25) == "0_25" and dl.fmt_num(1e6) == "1000000"


@pytest.mark.parametrize("variable", ["depth", "velocity", "pressure"])
@pytest.mark.parametrize("threshold", [0.01, 0.05, 0.1, 0.25, 1.0, 1.25, 2.5, 10.0, 1000.0])
def test_band_names_roundtrip(variable, threshold):
    assert dl.parse_band("return_period", dl.return_period_column(variable, threshold)) == {"threshold": threshold}
    assert dl.parse_band("hit_rate", dl.hit_rate_column(variable, threshold)) == {"threshold": threshold}


@pytest.mark.parametrize("rp", [10, 30, 100, 1000, 100000, 1000000])
def test_intensity_band_roundtrip(rp):
    assert dl.parse_band("intensity", dl.intensity_column("velocity", rp)) == {"return_period": rp}


def test_generic_hive_paths_with_root():
    assert dl.hive_prefix("m", (("variable", "a"),), "Scratch") == "Scratch/m/variable=a"
    assert dl.hive_mosaic_prefix("m", (), "Scratch") == "Scratch/m/_mosaic"
    assert dl.hive_unit_key("anriss_umhuellende", (), "batch_range", 3, "parquet") == \
        "Data-Lake-Derivate/anriss_umhuellende/batch_range=3/data.parquet"


@pytest.mark.parametrize("min_h", [0.5, 0.75, 1.0, 2.0])
def test_start_rate_band_roundtrip(min_h):
    assert dl.start_rate_column(min_h) == f"h_{dl.fmt_num(min_h)}m_startrate"
    assert dl.parse_band("start_rate", dl.start_rate_column(min_h)) == {"min_h": min_h}
