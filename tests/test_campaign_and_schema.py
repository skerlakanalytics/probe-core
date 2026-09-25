"""Facts about the data: these values must match what the lake was built with."""

import importlib

from probe_core import campaign
from probe_core.data_lake import data_interface, data_lake_schema


def test_campaign_values():
    assert len(data_interface._PHYSICS_GRID) == 36
    assert campaign.EVENTS_PER_BATCH == 100
    assert campaign.EVENT_MANIFEST_KEY == "maxi_event_manifest.parquet"


def test_event_manifest_follows_gold_bucket(monkeypatch):
    monkeypatch.delenv("PROBE_S3_BUCKET_GOLD", raising=False)
    di = importlib.reload(data_interface)
    assert di.EVENT_MANIFEST_S3_URI == "s3://maxi/maxi_event_manifest.parquet"
    monkeypatch.setenv("PROBE_S3_BUCKET_GOLD", "adelboden-test")
    di = importlib.reload(data_interface)
    assert di.EVENT_MANIFEST_S3_URI == "s3://adelboden-test/maxi_event_manifest.parquet"
    monkeypatch.delenv("PROBE_S3_BUCKET_GOLD")
    importlib.reload(data_interface)


def test_constants_from_packaged_com1dfa_template():
    assert data_lake_schema.PRESSURE_RHO == 1800.0
    assert data_lake_schema.GRAVITY == 9.81
