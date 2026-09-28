"""Mode A's threshold, converted to gold's integer units, keeps pixels at exactly that value."""

import duckdb
import pytest

from probe_core.derivate.maxi_ifk_and_raster import _raw_threshold


@pytest.mark.parametrize("variable", ["depth", "velocity"])
def test_every_centimetre_threshold_converts_exactly(variable):
    # 0.07 / 0.01 == 7.000000000000001 without the rounding
    for cm in range(1, 601):
        assert _raw_threshold(variable, cm / 100) == cm


def test_pressure_needs_no_conversion():
    assert _raw_threshold("pressure", 100.0) == 100


def test_pixel_at_the_threshold_is_included():
    con = duckdb.connect()
    included = con.execute(f"SELECT 7 >= {_raw_threshold('depth', 0.07)}").fetchone()[0]
    assert included is True


def test_threshold_between_stored_values_keeps_its_meaning():
    assert _raw_threshold("depth", 0.075) == 7.5
