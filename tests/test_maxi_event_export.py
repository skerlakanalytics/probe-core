"""Envelope cache: created on first write, written atomically, reused."""

import pandas as pd

from probe_core.derivate import maxi_event_export as mee


def test_envelope_cache_is_created_on_write_and_reused(tmp_path, monkeypatch):
    cache = tmp_path / "cache"
    monkeypatch.setattr(mee, "CACHE_DIR", cache)
    # two adjacent pixel centres (k*5 + 2.5)
    df = pd.DataFrame({"x": [2600002.5, 2600007.5], "y": [1200002.5, 1200002.5]})

    geojson, path = mee.export_event_envelope_geojson(df, 42)
    assert geojson["features"] and path == str(cache / "event_42_envelope.geojson")
    assert sorted(p.name for p in cache.iterdir()) == ["event_42_envelope.geojson"]   # no temp file left

    again, _ = mee.export_event_envelope_geojson(df.iloc[0:0], 42)   # empty df: must come from the cache
    assert again == geojson
