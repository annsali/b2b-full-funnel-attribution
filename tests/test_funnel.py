"""
Unit tests for funnel engine calculations.
"""
import sqlite3
import pytest
import pandas as pd
import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from src.funnel_engine import (
    run_funnel_waterfall, run_stage_durations, run_monthly_velocity, run_conversion_by_source
)


@pytest.fixture(scope="module")
def conn():
    c = sqlite3.connect(config.DB_PATH)
    yield c
    c.close()


class TestFunnelWaterfall:
    def test_returns_dataframe(self, conn):
        df = run_funnel_waterfall(conn)
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    def test_funnel_narrows(self, conn):
        """Each stage must have <= contacts than the previous stage."""
        df = run_funnel_waterfall(conn).iloc[0]
        assert df["total_mql"] <= df["total_leads"],  "MQL > Leads"
        assert df["total_sql"] <= df["total_mql"],    "SQL > MQL"
        assert df["total_sal"] <= df["total_sql"],    "SAL > SQL"
        assert df["total_opp"] <= df["total_sal"],    "Opp > SAL"
        assert df["total_won"] <= df["total_opp"],    "Won > Opp"

    def test_conversion_rates_are_valid(self, conn):
        df = run_funnel_waterfall(conn).iloc[0]
        for col in ["lead_to_mql", "mql_to_sql", "sql_to_sal", "sal_to_opp", "opp_to_won"]:
            rate = df[col]
            assert 0.0 <= rate <= 1.0, f"{col}={rate} out of [0,1]"

    def test_cumulative_rate_lte_individual(self, conn):
        df = run_funnel_waterfall(conn).iloc[0]
        assert df["cumulative_top_to_won"] <= df["opp_to_won"]

    def test_total_leads_positive(self, conn):
        df = run_funnel_waterfall(conn).iloc[0]
        assert df["total_leads"] > 1000


class TestStageDurations:
    def test_returns_expected_stages(self, conn):
        df = run_stage_durations(conn)
        stages = df["stage_name"].tolist()
        # At minimum MQL, SQL, SAL must have duration data
        for s in ["MQL", "SQL", "SAL"]:
            assert s in stages

    def test_avg_days_positive(self, conn):
        df = run_stage_durations(conn)
        assert (df["avg_days"] >= 0).all()

    def test_p75_gte_median(self, conn):
        df = run_stage_durations(conn).dropna(subset=["median_approx", "p75_days"])
        for _, row in df.iterrows():
            assert row["p75_days"] >= row["median_approx"] - 1, (
                f"{row['stage_name']}: p75={row['p75_days']} < median={row['median_approx']}"
            )


class TestMonthlyVelocity:
    def test_returns_months(self, conn):
        df = run_monthly_velocity(conn)
        assert len(df) >= 12   # at least 12 months of data

    def test_month_format(self, conn):
        df = run_monthly_velocity(conn)
        assert df["month"].str.match(r"^\d{4}-\d{2}$").all()

    def test_counts_non_negative(self, conn):
        df = run_monthly_velocity(conn)
        for col in ["new_leads", "mqls", "sqls", "opps"]:
            assert (df[col] >= 0).all()


class TestConversionBySource:
    def test_all_sources_present(self, conn):
        df = run_conversion_by_source(conn)
        assert len(df) >= 5

    def test_revenue_positive(self, conn):
        df = run_conversion_by_source(conn)
        assert df["attributed_revenue"].sum() > 0

    def test_win_rate_valid(self, conn):
        df = run_conversion_by_source(conn).dropna(subset=["win_rate"])
        assert (df["win_rate"].between(0, 1)).all()
