"""Facts about the data: these values must match what the lake was built with."""

import importlib

from probe_core import campaign
from probe_core.data_lake import data_interface, data_lake_schema
from probe_core.derivate import build_raster_derivate as brd


def test_campaign_values():
    assert len(data_interface._PHYSICS_GRID) == 36
    assert campaign.EVENTS_PER_BATCH == 100
    assert campaign.EVENT_MANIFEST_KEY == "maxi_event_manifest.parquet"


def test_input_bucket_default_and_override(monkeypatch):
    monkeypatch.delenv("PROBE_S3_BUCKET_INPUT", raising=False)
    di = importlib.reload(data_interface)
    assert di.EVENT_MANIFEST_S3_URI == "s3://input/maxi_event_manifest.parquet"
    monkeypatch.setenv("PROBE_S3_BUCKET_INPUT", "other-input")
    di = importlib.reload(data_interface)
    assert di.EVENT_MANIFEST_S3_URI == "s3://other-input/maxi_event_manifest.parquet"
    monkeypatch.delenv("PROBE_S3_BUCKET_INPUT")
    importlib.reload(data_interface)


def test_constants_from_packaged_com1dfa_template():
    assert data_lake_schema.PRESSURE_RHO == 1800.0
    assert data_lake_schema.GRAVITY == 9.81


# The raster-derivate matrix of ProBE_control_center config/jobs.yaml
# (2026-09-18). Its hash names the live S3 prefix Data-Lake-Derivate/raster/cfg_a6874165
# in both "maxi" and "adelboden-test": if config_hash() changes, the app no
# longer finds the precomputed rasters and the pipeline recomputes everything.
THRESHOLDS = (
    [{"variable": "depth", "threshold": t} for t in (0.25, 0.5, 1.0, 1.25, 1.5, 2.0)]
    + [{"variable": "velocity", "threshold": t} for t in (0.5, 1.0, 2.0, 2.5, 3.0, 4.0)]
    + [{"variable": "pressure", "threshold": t} for t in (50.0, 100.0, 125.0, 150.0, 200.0, 250.0)]
)
RETURN_PERIODS = [{"return_period": rp} for rp in (100, 1000, 10000, 100000, 1000000)]


def test_config_hash_matches_live_prefix():
    assert brd.config_hash(THRESHOLDS, RETURN_PERIODS) == "a6874165"


def test_config_hash_ignores_order():
    assert brd.config_hash(THRESHOLDS[::-1], RETURN_PERIODS[::-1]) == "a6874165"


def test_column_names():
    assert brd.return_period_column("depth", 1.0) == "depth_1m_rp"
    assert brd.intensity_column("depth", 1000000) == "depth_rp1000000y"
    assert brd._fmt_num(0.25) == "0_25"
