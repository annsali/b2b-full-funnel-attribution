"""
Integration test — runs the full pipeline and verifies all exports exist
and have expected shape.
"""
import os, sqlite3, pytest, sys
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from src.funnel_engine import run_all as funnel_all
from src.attribution_models import run_all_models, get_channel_comparison
from src.lead_velocity import run_all as velocity_all
from src.cohort_analysis import run_all as cohort_all
from src.revenue_attribution import run_all as revenue_all
from src.tableau_exporter import run_all_exports

EXPORT_DIR = "dashboards/tableau_data"

EXPECTED_EXPORTS = [
    "funnel_summary.csv",
    "attribution_by_channel.csv",
    "attribution_by_campaign.csv",
    "lead_velocity_trending.csv",
    "cohort_conversion_matrix.csv",
    "stage_duration_distribution.csv",
    "executive_kpis.csv",
    "segment_performance.csv",
]

REQUIRED_COLUMNS = {
    "funnel_summary.csv":          ["month", "stage", "count"],
    "attribution_by_channel.csv":  ["channel", "model", "attributed_revenue"],
    "attribution_by_campaign.csv": ["campaign_id", "attributed_revenue"],
    "lead_velocity_trending.csv":  ["month", "mqls"],
    "cohort_conversion_matrix.csv":["cohort_month", "cohort_size"],
    "stage_duration_distribution.csv": ["stage_name", "days_in_stage"],
    "executive_kpis.csv":          ["kpi_name", "current_value"],
    "segment_performance.csv":     ["industry", "company_size"],
}


@pytest.fixture(scope="module")
def conn():
    c = sqlite3.connect(config.DB_PATH)
    yield c
    c.close()


@pytest.fixture(scope="module")
def run_exports(conn):
    run_all_exports(conn)


class TestDatabaseExists:
    def test_db_file_exists(self):
        assert os.path.exists(config.DB_PATH), f"Database not found: {config.DB_PATH}"

    def test_all_tables_exist(self, conn):
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cursor.fetchall()}
        for t in ["accounts", "contacts", "touchpoints", "opportunities",
                  "lead_stages", "campaigns"]:
            assert t in tables, f"Missing table: {t}"


class TestTableRowCounts:
    def test_accounts_count(self, conn):
        n = pd.read_sql_query("SELECT COUNT(*) AS n FROM accounts", conn)["n"].iloc[0]
        assert n >= 20_000, f"Only {n} accounts"

    def test_contacts_count(self, conn):
        n = pd.read_sql_query("SELECT COUNT(*) AS n FROM contacts", conn)["n"].iloc[0]
        assert n >= 50_000, f"Only {n} contacts"

    def test_touchpoints_count(self, conn):
        n = pd.read_sql_query("SELECT COUNT(*) AS n FROM touchpoints", conn)["n"].iloc[0]
        assert n >= 200_000, f"Only {n} touchpoints"

    def test_opportunities_count(self, conn):
        n = pd.read_sql_query("SELECT COUNT(*) AS n FROM opportunities", conn)["n"].iloc[0]
        assert n >= 1_000, f"Only {n} opportunities"


class TestFunnelEngine:
    def test_funnel_runs(self, conn):
        results = funnel_all(conn)
        assert "funnel_waterfall" in results
        assert len(results["funnel_waterfall"]) > 0


class TestAttributionModels:
    def test_six_models_run(self, conn):
        df = run_all_models(conn)
        assert len(df["model"].unique()) == 6

    def test_revenue_positive(self, conn):
        df = run_all_models(conn)
        assert df["attributed_revenue"].sum() > 0


class TestLeadVelocity:
    def test_velocity_runs(self, conn):
        results = velocity_all(conn)
        assert "lvr" in results
        assert len(results["lvr"]) >= 12


class TestCohortAnalysis:
    def test_cohorts_run(self, conn):
        results = cohort_all(conn)
        assert "acquisition_cohorts" in results
        assert len(results["acquisition_cohorts"]) >= 12


class TestRevenueAttribution:
    def test_revenue_runs(self, conn):
        results = revenue_all(conn)
        assert "channel_revenue" in results
        assert len(results["channel_revenue"]) >= 5


class TestTableauExports:
    def test_all_files_exist(self, conn, run_exports):
        for fname in EXPECTED_EXPORTS:
            path = os.path.join(EXPORT_DIR, fname)
            assert os.path.exists(path), f"Missing export: {path}"

    def test_non_zero_rows(self, conn, run_exports):
        for fname in EXPECTED_EXPORTS:
            path = os.path.join(EXPORT_DIR, fname)
            if not os.path.exists(path):
                continue
            df = pd.read_csv(path)
            assert len(df) > 0, f"{fname} is empty"

    def test_required_columns(self, conn, run_exports):
        for fname, cols in REQUIRED_COLUMNS.items():
            path = os.path.join(EXPORT_DIR, fname)
            if not os.path.exists(path):
                continue
            df = pd.read_csv(path)
            for col in cols:
                assert col in df.columns, f"{fname} missing column '{col}'"
