"""Unit tests for cohort analysis."""
import pytest
import pandas as pd
import numpy as np
import sqlite3
import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from src.cohort_analysis import build_acquisition_cohorts, build_channel_cohorts


@pytest.fixture(scope="module")
def conn():
    c = sqlite3.connect(config.DB_PATH)
    yield c
    c.close()


class TestAcquisitionCohorts:
    def test_returns_dataframe(self, conn):
        df = build_acquisition_cohorts(conn)
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    def test_cohort_month_format(self, conn):
        df = build_acquisition_cohorts(conn)
        assert df["cohort_month"].str.match(r"^\d{4}-\d{2}$").all()

    def test_sql_lte_cohort_size(self, conn):
        df = build_acquisition_cohorts(conn)
        assert (df["sql_count"] <= df["cohort_size"]).all()

    def test_opp_lte_sql(self, conn):
        df = build_acquisition_cohorts(conn)
        assert (df["opp_count"] <= df["sql_count"]).all()

    def test_won_lte_opp(self, conn):
        df = build_acquisition_cohorts(conn)
        assert (df["won_count"] <= df["opp_count"]).all()

    def test_conversion_rate_in_range(self, conn):
        df = build_acquisition_cohorts(conn)
        for col in ["mql_to_sql_rate", "mql_to_opp_rate", "mql_to_won_rate"]:
            valid = df[col].dropna()
            assert (valid.between(0, 1)).all(), f"{col} out of [0,1]"

    def test_cumulative_conversion_never_exceeds_100pct(self, conn):
        df = build_acquisition_cohorts(conn)
        assert (df["mql_to_won_rate"].dropna() <= 1.0).all()

    def test_at_least_12_cohorts(self, conn):
        df = build_acquisition_cohorts(conn)
        assert len(df) >= 12, f"Expected >=12 cohort months, got {len(df)}"


class TestChannelCohorts:
    def test_returns_dataframe(self, conn):
        df = build_channel_cohorts(conn)
        assert isinstance(df, pd.DataFrame)

    def test_win_rate_valid(self, conn):
        df = build_channel_cohorts(conn)
        valid = df["win_rate"].dropna()
        assert (valid.between(0, 1)).all()

    def test_won_lte_opp(self, conn):
        df = build_channel_cohorts(conn)
        assert (df["won_count"] <= df["opp_count"]).all()

    def test_all_channels_present(self, conn):
        df = build_channel_cohorts(conn)
        expected = {"Paid_Search", "Paid_Social", "Email", "Events", "Webinar"}
        found    = set(df["channel"].tolist())
        missing  = expected - found
        assert len(missing) == 0, f"Missing channels: {missing}"
