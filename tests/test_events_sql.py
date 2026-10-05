"""_events_sql: the gold x probability_lookup x ablauf join behind every IFK curve and raster."""

import duckdb
import pytest

from probe_core.data_lake.data_lake_schema import GOLD_P_H_MAX, GOLD_P_H_MEAN
from probe_core.derivate import maxi_ifk_and_raster as m


@pytest.fixture()
def con():
    con = duckdb.connect()
    # Two simulations of anriss 1 (mean and max thickness), one of anriss 2 whose
    # friction combination is not in ablauf, one of anriss 3 that is not in the lookup.
    con.execute("""
        CREATE TABLE gold AS SELECT * FROM (VALUES
            (1, 100, 100, 0.05, 100, 1000, 12),
            (1, 140, 100, 0.05, 100, 1000, 30),
            (2, 100, 100, 0.99, 100, 1000, 5),
            (3, 100, 100, 0.05, 100, 1000, 7)
        ) AS t(id_anriss, h, d, mu, xsi, tau0, "Fliesstiefe")
    """)
    con.execute("""
        CREATE TABLE probability_lookup AS SELECT * FROM (VALUES
            (1, 0.01, 0.5, 0.25), (2, 0.02, 0.5, 0.25)
        ) AS t(id_anriss, lambda_Hangmuren, p_raeumlich, "p_A")
    """)
    con.execute("CREATE TABLE ablauf AS SELECT 0.05 AS mu, 100 AS xsi, 1000 AS tau0, 0.1 AS p_ablauf")
    return con


def test_rate_is_the_product_of_the_five_factors(con):
    rows = con.execute(f"SELECT h, p_h::DOUBLE, lambda_ereignis::DOUBLE FROM ({m._events_sql('gold')}) ORDER BY h").fetchall()
    assert rows == [(100, pytest.approx(GOLD_P_H_MEAN), pytest.approx(0.01 * 0.5 * 0.25 * GOLD_P_H_MEAN * 0.1)),
                    (140, pytest.approx(GOLD_P_H_MAX), pytest.approx(0.01 * 0.5 * 0.25 * GOLD_P_H_MAX * 0.1))]


def test_keeps_the_gold_columns_and_adds_the_factors(con):
    columns = [c[0] for c in con.execute(m._events_sql('gold')).description]
    assert columns == ['id_anriss', 'h', 'd', 'mu', 'xsi', 'tau0', 'Fliesstiefe',
                       'lambda_Hangmuren', 'p_raeumlich', 'p_A', 'p_h', 'p_ablauf', 'lambda_ereignis']


def test_expert_mode_overrides_p_h(con):
    p_h = con.execute(f"SELECT p_h::DOUBLE FROM ({m._events_sql('gold', p_h_mean=0.3, p_h_max=0.0)}) ORDER BY h").fetchall()
    assert p_h == [(pytest.approx(0.3),), (pytest.approx(0.0),)]
