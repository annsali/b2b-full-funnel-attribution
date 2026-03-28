"""
Unit tests for all 6 attribution models.
Verifies credit sums to 1.0 per contact and model-specific logic.
"""
import pytest
import pandas as pd
import numpy as np
import sqlite3
import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from src.attribution_models import (
    first_touch, last_touch, linear, time_decay,
    position_based, data_driven, load_data,
    run_all_models, get_channel_comparison, model_agreement_score,
)


# ---------------------------------------------------------------------------
# Synthetic micro-dataset for deterministic unit tests
# ---------------------------------------------------------------------------

def make_mini_data():
    """3-touchpoint journey for a single closed-won contact."""
    tps = pd.DataFrame([
        {"touchpoint_id": "T1", "contact_id": "C1", "account_id": "A1",
         "channel": "Paid_Search",  "touchpoint_timestamp": "2024-01-01T10:00:00",
         "cost": 50.0, "campaign_id": "X"},
        {"touchpoint_id": "T2", "contact_id": "C1", "account_id": "A1",
         "channel": "Email",        "touchpoint_timestamp": "2024-01-15T10:00:00",
         "cost": None, "campaign_id": "X"},
        {"touchpoint_id": "T3", "contact_id": "C1", "account_id": "A1",
         "channel": "Events",       "touchpoint_timestamp": "2024-02-01T10:00:00",
         "cost": None, "campaign_id": "X"},
    ])
    opps = pd.DataFrame([{
        "contact_id":   "C1",
        "opportunity_id": "O1",
        "amount":       10_000,
        "conv_date":    "2024-02-05",
    }])
    return tps, opps


@pytest.fixture
def mini():
    return make_mini_data()


class TestFirstTouch:
    def test_all_credit_to_first(self, mini):
        tps, opps = mini
        result = first_touch(tps, opps)
        ps_row = result[result["channel"] == "Paid_Search"]
        assert len(ps_row) == 1
        assert ps_row.iloc[0]["attributed_revenue"] == 10_000

    def test_total_revenue_matches(self, mini):
        tps, opps = mini
        result = first_touch(tps, opps)
        assert abs(result["attributed_revenue"].sum() - 10_000) < 1

    def test_non_first_channels_zero(self, mini):
        tps, opps = mini
        result = first_touch(tps, opps)
        for ch in ["Email", "Events"]:
            row = result[result["channel"] == ch]
            assert len(row) == 0 or row.iloc[0]["attributed_revenue"] == 0


class TestLastTouch:
    def test_all_credit_to_last(self, mini):
        tps, opps = mini
        result = last_touch(tps, opps)
        ev_row = result[result["channel"] == "Events"]
        assert len(ev_row) == 1
        assert ev_row.iloc[0]["attributed_revenue"] == 10_000

    def test_total_matches(self, mini):
        tps, opps = mini
        assert abs(last_touch(tps, opps)["attributed_revenue"].sum() - 10_000) < 1


class TestLinear:
    def test_equal_split(self, mini):
        tps, opps = mini
        result = linear(tps, opps)
        # 3 touches → each gets 1/3 = $3,333
        for ch in ["Paid_Search", "Email", "Events"]:
            row = result[result["channel"] == ch]
            assert len(row) == 1
            assert abs(row.iloc[0]["attributed_revenue"] - 10_000 / 3) < 1

    def test_total_matches(self, mini):
        tps, opps = mini
        assert abs(linear(tps, opps)["attributed_revenue"].sum() - 10_000) < 1

    def test_single_touchpoint(self):
        tps = pd.DataFrame([{
            "touchpoint_id": "T1", "contact_id": "C1", "account_id": "A1",
            "channel": "Direct", "touchpoint_timestamp": "2024-01-01T10:00:00",
            "cost": None, "campaign_id": "X"
        }])
        opps = pd.DataFrame([{"contact_id": "C1", "opportunity_id": "O1",
                               "amount": 5000, "conv_date": "2024-01-30"}])
        result = linear(tps, opps)
        assert abs(result["attributed_revenue"].sum() - 5000) < 1


class TestTimeDecay:
    def test_last_touch_gets_most_credit(self, mini):
        """Events (T3) is closest to conversion → should get highest weight."""
        tps, opps = mini
        result = time_decay(tps, opps)
        events_rev = result[result["channel"] == "Events"].iloc[0]["attributed_revenue"]
        ps_rev     = result[result["channel"] == "Paid_Search"].iloc[0]["attributed_revenue"]
        assert events_rev > ps_rev

    def test_weights_sum_to_one(self, mini):
        tps, opps = mini
        result = time_decay(tps, opps)
        assert abs(result["attributed_revenue"].sum() - 10_000) < 1

    def test_decay_is_monotone_with_distance(self):
        """Closer touchpoints must have higher weight."""
        tps = pd.DataFrame([
            {"touchpoint_id": "T1", "contact_id": "C1", "account_id": "A1",
             "channel": "A", "touchpoint_timestamp": "2024-01-01T10:00:00",
             "cost": None, "campaign_id": "X"},
            {"touchpoint_id": "T2", "contact_id": "C1", "account_id": "A1",
             "channel": "B", "touchpoint_timestamp": "2024-01-20T10:00:00",
             "cost": None, "campaign_id": "X"},
            {"touchpoint_id": "T3", "contact_id": "C1", "account_id": "A1",
             "channel": "C", "touchpoint_timestamp": "2024-01-28T10:00:00",
             "cost": None, "campaign_id": "X"},
        ])
        opps = pd.DataFrame([{"contact_id": "C1", "opportunity_id": "O1",
                               "amount": 9000, "conv_date": "2024-01-30"}])
        result = time_decay(tps, opps)
        rev = {row["channel"]: row["attributed_revenue"] for _, row in result.iterrows()}
        assert rev["C"] > rev["B"] > rev["A"], f"Decay not monotone: {rev}"


class TestPositionBased:
    def test_40_40_20_split(self, mini):
        tps, opps = mini
        result = position_based(tps, opps)
        ps  = result[result["channel"] == "Paid_Search"].iloc[0]["attributed_revenue"]
        ev  = result[result["channel"] == "Events"].iloc[0]["attributed_revenue"]
        em  = result[result["channel"] == "Email"].iloc[0]["attributed_revenue"]
        assert abs(ps - 4000) < 1,   f"First touch should be $4000, got {ps}"
        assert abs(ev - 4000) < 1,   f"Last touch should be $4000, got {ev}"
        assert abs(em - 2000) < 1,   f"Middle should be $2000, got {em}"

    def test_one_touchpoint(self):
        tps = pd.DataFrame([{"touchpoint_id": "T1", "contact_id": "C1",
                              "account_id": "A1", "channel": "Direct",
                              "touchpoint_timestamp": "2024-01-01T10:00:00",
                              "cost": None, "campaign_id": "X"}])
        opps = pd.DataFrame([{"contact_id": "C1", "opportunity_id": "O1",
                               "amount": 8000, "conv_date": "2024-01-20"}])
        result = position_based(tps, opps)
        assert abs(result["attributed_revenue"].sum() - 8000) < 1

    def test_two_touchpoints(self):
        tps = pd.DataFrame([
            {"touchpoint_id": "T1", "contact_id": "C1", "account_id": "A1",
             "channel": "A", "touchpoint_timestamp": "2024-01-01T10:00:00",
             "cost": None, "campaign_id": "X"},
            {"touchpoint_id": "T2", "contact_id": "C1", "account_id": "A1",
             "channel": "B", "touchpoint_timestamp": "2024-01-15T10:00:00",
             "cost": None, "campaign_id": "X"},
        ])
        opps = pd.DataFrame([{"contact_id": "C1", "opportunity_id": "O1",
                               "amount": 6000, "conv_date": "2024-01-20"}])
        result = position_based(tps, opps)
        assert abs(result["attributed_revenue"].sum() - 6000) < 1

    def test_total_matches(self, mini):
        tps, opps = mini
        assert abs(position_based(tps, opps)["attributed_revenue"].sum() - 10_000) < 1


class TestDataDriven:
    def test_returns_dataframe(self, mini):
        tps, opps = mini
        result = data_driven(tps, opps)
        assert isinstance(result, pd.DataFrame)

    def test_total_revenue_approximately_correct(self):
        # Using real DB
        conn = sqlite3.connect(config.DB_PATH)
        tps, opps = load_data(conn)
        result = data_driven(tps, opps, conn)
        total_actual = opps["amount"].sum()
        total_attr   = result["attributed_revenue"].sum()
        assert abs(total_attr - total_actual) / total_actual < 0.02, (
            f"Data-driven total {total_attr:,.0f} deviates >2% from actual {total_actual:,.0f}"
        )
        conn.close()


class TestAllModels:
    def test_run_all_returns_six_models(self):
        conn = sqlite3.connect(config.DB_PATH)
        results = run_all_models(conn)
        models = results["model"].unique()
        assert len(models) == 6, f"Expected 6 models, got: {models}"
        conn.close()

    def test_channel_comparison_shape(self):
        conn = sqlite3.connect(config.DB_PATH)
        wide = get_channel_comparison(conn)
        assert "channel" in wide.columns
        assert len(wide) >= 5   # at least 5 channels
        conn.close()

    def test_model_agreement_score_is_valid(self):
        conn = sqlite3.connect(config.DB_PATH)
        wide  = get_channel_comparison(conn)
        score = model_agreement_score(wide)
        assert 0.0 <= score <= 1.0
        conn.close()
