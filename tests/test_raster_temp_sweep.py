"""sweep_raster_temp: what a killed raster job leaves in the DuckDB temp folder."""

import os

from probe_core.derivate.maxi_ifk_and_raster import RASTER_TEMP_MAX_AGE_S, sweep_raster_temp

NOW = 2_000_000_000.0


def _make(path, age_s, is_dir=False):
    if is_dir:
        path.mkdir()
        (path / "work.db").write_bytes(b"x")
    else:
        path.write_bytes(b"x")
    os.utime(path, (NOW - age_s, NOW - age_s))


def test_deletes_what_is_older_than_seven_days_and_keeps_the_rest(tmp_path):
    old = RASTER_TEMP_MAX_AGE_S + 60
    _make(tmp_path / "raster_work_1_old", old, is_dir=True)          # a killed job's folder
    _make(tmp_path / "raster_work_1_old.db", old)                     # pre per-job-folder leftovers
    _make(tmp_path / "duckdb_temp_storage_S160K-0.tmp", old)
    _make(tmp_path / "raster_work_2_running", 3600, is_dir=True)     # a running job's folder

    assert sweep_raster_temp(str(tmp_path), now=NOW) == 3
    assert [p.name for p in tmp_path.iterdir()] == ["raster_work_2_running"]
