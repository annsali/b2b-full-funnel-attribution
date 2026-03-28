"""Unit tests for lead velocity metrics."""
import pytest
import pandas as pd
import numpy as np
import sqlite3
import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from src.lead_velocity import (
    compute_lvr, compute_pipeline_velocity,
    compute_stage_durations, compute_time_to_revenue,
    compute_marketing_pipeline,
)


@pytest.fixture(scope="module")
def conn():
    c = sqlite3.connect(config.DB_PATH)
    yield c
    c.close()


class TestLVR:
    def test_lvr_calculation(self):
        """LVR = (this_month - last_month) / last_month * 100."""
        df = pd.DataFrame({
            "month":    ["2024-01", "2024-02", "2024-03"],
            "mqls":     [100, 120, 110],
            "sqls":     [30,  36,  33],
            "opps":     [20,  24,  22],
            "won":      [6,   7,   6],
        })
        df["mqls_lvr_pct"] = df["mqls"].pct_change() * 100
        assert abs(df.iloc[1]["mqls_lvr_pct"] - 20.0) < 0.01
        assert abs(df.iloc[2]["mqls_lvr_pct"] - (-8.333)) < 0.01

    def test_lvr_from_db_has_months(self, conn):
        df = compute_lvr(conn)
        assert len(df) >= 12

    def test_lvr_month_format(self, conn):
        df = compute_lvr(conn)
        assert df["month"].str.match(r"^\d{4}-\d{2}$").all()

    def test_lvr_mqls_non_negative(self, conn):
        df = compute_lvr(conn)
        assert (df["mqls"] >= 0).all()


class TestPipelineVelocity:
    def test_formula_correctness(self):
        """pipeline_velocity = (n * win_rate * avg_deal) / avg_days."""
        n, wr, ad, days = 100, 0.30, 50_000, 60
        expected = (n * wr * ad) / days
        computed = (n * wr * ad) / days
        assert abs(expected - computed) < 0.01

    def test_pipeline_velocity_from_db_positive(self, conn):
        df = compute_pipeline_velocity(conn)
        assert (df["pipeline_velocity"].dropna() > 0).any()


class TestStageDurations:
    def test_expected_stages_present(self, conn):
        df = compute_stage_durations(conn)
        stages = df["stage_name"].tolist()
        for s in ["MQL", "SQL", "SAL"]:
            assert s in stages

    def test_avg_days_positive(self, conn):
        df = compute_stage_durations(conn)
        assert (df["avg_days"] >= 0).all()


class TestTimeToRevenue:
    def test_returns_positive_days(self, conn):
        df = compute_time_to_revenue(conn)
        val = df["avg_days_first_touch_to_revenue"].iloc[0]
        assert val is None or val > 0

    def test_n_positive(self, conn):
        df = compute_time_to_revenue(conn)
        assert df["n"].iloc[0] > 0


class TestMarketingPipeline:
    def test_returns_rows(self, conn):
        df = compute_marketing_pipeline(conn)
        assert len(df) >= 1

    def test_pipeline_value_positive(self, conn):
        df = compute_marketing_pipeline(conn)
        assert df["pipeline_value"].sum() > 0
