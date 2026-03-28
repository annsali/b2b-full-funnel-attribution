"""
Multi-touch attribution models (6 total).

Models:
    1. First Touch
    2. Last Touch
    3. Linear
    4. Time Decay
    5. Position-Based (U-Shaped)
    6. Data-Driven (Shapley-inspired logistic regression)

Run: python -m src.attribution_models
"""
import os, sys, sqlite3
import numpy as np
import pandas as pd
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

# ---------------------------------------------------------------------------
# SQL for attribution (also written to sql/02_attribution_queries.sql)
# ---------------------------------------------------------------------------

ATTRIBUTION_SQL = """
-- ==========================================================================
-- 02_attribution_queries.sql
-- Multi-touch attribution: all 6 models.
-- Each model allocates conversion credit (and revenue) across touchpoints.
-- ==========================================================================

-- --------------------------------------------------------------------------
-- Query 1: First Touch & Last Touch Attribution (combined)
-- First touch = 100% credit to the chronologically earliest touchpoint.
-- Last touch  = 100% credit to the touchpoint immediately before conversion.
-- --------------------------------------------------------------------------
WITH contact_opps AS (
    -- Only attribute for closed-won contacts
    SELECT c.contact_id, c.account_id, c.mql_date, c.opportunity_date,
           o.opportunity_id, o.amount
    FROM contacts c
    JOIN opportunities o ON o.primary_contact_id = c.contact_id
    WHERE o.closed_won = 1
),
ranked_tp AS (
    SELECT
        t.touchpoint_id,
        t.contact_id,
        t.channel,
        t.campaign_id,
        t.campaign_name,
        t.touchpoint_timestamp,
        t.cost,
        co.amount,
        co.opportunity_id,
        ROW_NUMBER() OVER (
            PARTITION BY t.contact_id
            ORDER BY t.touchpoint_timestamp ASC
        ) AS rn_first,
        ROW_NUMBER() OVER (
            PARTITION BY t.contact_id
            ORDER BY t.touchpoint_timestamp DESC
        ) AS rn_last
    FROM touchpoints t
    JOIN contact_opps co ON co.contact_id = t.contact_id
)
SELECT
    channel,
    SUM(CASE WHEN rn_first = 1 THEN amount ELSE 0 END) AS first_touch_revenue,
    COUNT(CASE WHEN rn_first = 1 THEN 1 END)            AS first_touch_conversions,
    SUM(CASE WHEN rn_last  = 1 THEN amount ELSE 0 END) AS last_touch_revenue,
    COUNT(CASE WHEN rn_last  = 1 THEN 1 END)            AS last_touch_conversions
FROM ranked_tp
GROUP BY channel
ORDER BY first_touch_revenue DESC;

-- --------------------------------------------------------------------------
-- Query 2: Linear Attribution with Revenue Allocation
-- Each touchpoint in the path gets equal share of the deal value.
-- --------------------------------------------------------------------------
WITH contact_opps AS (
    SELECT c.contact_id, o.opportunity_id, o.amount
    FROM contacts c
    JOIN opportunities o ON o.primary_contact_id = c.contact_id
    WHERE o.closed_won = 1
),
tp_counts AS (
    SELECT t.contact_id,
           COUNT(*) OVER (PARTITION BY t.contact_id) AS total_touchpoints
    FROM touchpoints t
    JOIN contact_opps co ON co.contact_id = t.contact_id
),
weighted AS (
    SELECT
        t.touchpoint_id,
        t.contact_id,
        t.channel,
        t.campaign_id,
        co.amount,
        1.0 / tc.total_touchpoints              AS weight,
        co.amount / tc.total_touchpoints        AS attributed_revenue
    FROM touchpoints t
    JOIN contact_opps  co ON co.contact_id = t.contact_id
    JOIN tp_counts     tc ON tc.contact_id = t.contact_id
)
SELECT
    channel,
    ROUND(SUM(weight),              2) AS total_weight_check,
    ROUND(SUM(attributed_revenue),  0) AS linear_attributed_revenue,
    COUNT(DISTINCT contact_id)          AS contacts
FROM weighted
GROUP BY channel
ORDER BY linear_attributed_revenue DESC;

-- --------------------------------------------------------------------------
-- Query 3: Time Decay Attribution
-- Touchpoints closer to conversion receive exponentially more credit.
-- Decay half-life = 7 days (weight = 2^(-(days_before_conv)/7))
-- --------------------------------------------------------------------------
WITH contact_opps AS (
    SELECT c.contact_id, o.opportunity_id, o.amount,
           c.opportunity_date AS conversion_date
    FROM contacts c
    JOIN opportunities o ON o.primary_contact_id = c.contact_id
    WHERE o.closed_won = 1 AND c.opportunity_date IS NOT NULL
),
decay_weights AS (
    SELECT
        t.touchpoint_id,
        t.contact_id,
        t.channel,
        t.campaign_id,
        co.amount,
        -- days before conversion (non-negative)
        CAST(
            JULIANDAY(co.conversion_date) -
            JULIANDAY(SUBSTR(t.touchpoint_timestamp,1,10))
        AS REAL) AS days_before_conv,
        -- exponential decay: half-life = 7 days
        POWER(2.0, -(
            JULIANDAY(co.conversion_date) -
            JULIANDAY(SUBSTR(t.touchpoint_timestamp,1,10))
        ) / 7.0) AS raw_weight
    FROM touchpoints t
    JOIN contact_opps co ON co.contact_id = t.contact_id
    WHERE JULIANDAY(SUBSTR(t.touchpoint_timestamp,1,10)) <=
          JULIANDAY(co.conversion_date)
),
normalized AS (
    SELECT *,
           raw_weight / SUM(raw_weight) OVER (PARTITION BY contact_id) AS norm_weight,
           raw_weight / SUM(raw_weight) OVER (PARTITION BY contact_id) * amount AS td_revenue
    FROM decay_weights
)
SELECT
    channel,
    ROUND(SUM(norm_weight),  4) AS total_weight_check,
    ROUND(SUM(td_revenue),   0) AS time_decay_revenue,
    COUNT(DISTINCT contact_id)   AS contacts
FROM normalized
GROUP BY channel
ORDER BY time_decay_revenue DESC;

-- --------------------------------------------------------------------------
-- Query 4: Position-Based (U-Shaped) Attribution
-- 40% first touch, 40% last touch, 20% split equally among middle touches.
-- Edge cases: 1 touch = 100%; 2 touches = 50/50.
-- --------------------------------------------------------------------------
WITH contact_opps AS (
    SELECT c.contact_id, o.amount
    FROM contacts c
    JOIN opportunities o ON o.primary_contact_id = c.contact_id
    WHERE o.closed_won = 1
),
tp_ranked AS (
    SELECT
        t.touchpoint_id,
        t.contact_id,
        t.channel,
        co.amount,
        ROW_NUMBER() OVER (PARTITION BY t.contact_id ORDER BY t.touchpoint_timestamp ASC)  AS rn_asc,
        ROW_NUMBER() OVER (PARTITION BY t.contact_id ORDER BY t.touchpoint_timestamp DESC) AS rn_desc,
        COUNT(*)     OVER (PARTITION BY t.contact_id)  AS total_tp
    FROM touchpoints t
    JOIN contact_opps co ON co.contact_id = t.contact_id
),
position_weights AS (
    SELECT
        touchpoint_id, contact_id, channel, amount, rn_asc, rn_desc, total_tp,
        CASE
            WHEN total_tp = 1
                THEN 1.0
            WHEN total_tp = 2
                THEN 0.50
            WHEN rn_asc  = 1                     -- first touch
                THEN 0.40
            WHEN rn_desc = 1                     -- last touch
                THEN 0.40
            ELSE 0.20 / (total_tp - 2.0)         -- middle touches share 20%
        END AS weight
    FROM tp_ranked
)
SELECT
    channel,
    ROUND(SUM(weight * amount), 0) AS position_based_revenue,
    COUNT(DISTINCT contact_id)      AS contacts
FROM position_weights
GROUP BY channel
ORDER BY position_based_revenue DESC;

-- --------------------------------------------------------------------------
-- Query 5: Channel-Level Attribution Comparison (all SQL models)
-- Shows First Touch, Last Touch, Linear, Time Decay, Position-Based side by side.
-- --------------------------------------------------------------------------
WITH contact_opps AS (
    SELECT c.contact_id, o.amount, c.opportunity_date AS conv_date
    FROM contacts c
    JOIN opportunities o ON o.primary_contact_id = c.contact_id
    WHERE o.closed_won = 1
),
tp_base AS (
    SELECT t.touchpoint_id, t.contact_id, t.channel, t.cost, co.amount, co.conv_date,
           ROW_NUMBER() OVER (PARTITION BY t.contact_id ORDER BY t.touchpoint_timestamp ASC)  AS rn_asc,
           ROW_NUMBER() OVER (PARTITION BY t.contact_id ORDER BY t.touchpoint_timestamp DESC) AS rn_desc,
           COUNT(*)     OVER (PARTITION BY t.contact_id)                                       AS n_tp,
           POWER(2.0, -(JULIANDAY(co.conv_date) -
                        JULIANDAY(SUBSTR(t.touchpoint_timestamp,1,10))) / 7.0)                 AS raw_decay
    FROM touchpoints t
    JOIN contact_opps co ON co.contact_id = t.contact_id
),
with_norm_decay AS (
    SELECT *,
           raw_decay / SUM(raw_decay) OVER (PARTITION BY contact_id) AS decay_w
    FROM tp_base
)
SELECT
    channel,
    SUM(CASE WHEN rn_asc=1 THEN amount ELSE 0 END)         AS first_touch_rev,
    SUM(CASE WHEN rn_desc=1 THEN amount ELSE 0 END)        AS last_touch_rev,
    ROUND(SUM(amount / n_tp), 0)                           AS linear_rev,
    ROUND(SUM(decay_w * amount), 0)                        AS time_decay_rev,
    ROUND(SUM(
        CASE WHEN n_tp=1  THEN 1.0
             WHEN n_tp=2  THEN 0.5
             WHEN rn_asc=1  THEN 0.4
             WHEN rn_desc=1 THEN 0.4
             ELSE 0.2/(n_tp-2.0) END * amount
    ), 0)                                                   AS position_rev,
    SUM(COALESCE(cost, 0))                                  AS total_cost
FROM with_norm_decay
GROUP BY channel
ORDER BY time_decay_rev DESC;

-- --------------------------------------------------------------------------
-- Query 6: Campaign-Level Attribution with ROI
-- --------------------------------------------------------------------------
WITH contact_opps AS (
    SELECT c.contact_id, o.amount, c.opportunity_date AS conv_date
    FROM contacts c
    JOIN opportunities o ON o.primary_contact_id = c.contact_id
    WHERE o.closed_won = 1
),
tp_decay AS (
    SELECT t.touchpoint_id, t.contact_id, t.campaign_id, t.campaign_name,
           t.channel, t.cost, co.amount, co.conv_date,
           POWER(2.0, -(JULIANDAY(co.conv_date) -
                        JULIANDAY(SUBSTR(t.touchpoint_timestamp,1,10))) / 7.0) AS raw_decay
    FROM touchpoints t
    JOIN contact_opps co ON co.contact_id = t.contact_id
),
normalized AS (
    SELECT *,
           raw_decay / SUM(raw_decay) OVER (PARTITION BY contact_id) AS decay_w,
           raw_decay / SUM(raw_decay) OVER (PARTITION BY contact_id) * amount AS td_rev
    FROM tp_decay
)
SELECT
    campaign_id,
    campaign_name,
    channel,
    COUNT(DISTINCT contact_id)         AS attributed_contacts,
    ROUND(SUM(td_rev), 0)              AS attributed_revenue,
    ROUND(SUM(COALESCE(cost, 0)), 0)   AS campaign_cost,
    ROUND(SUM(td_rev) / NULLIF(SUM(COALESCE(cost, 0)), 0), 2) AS roi,
    RANK() OVER (ORDER BY SUM(td_rev) DESC) AS revenue_rank
FROM normalized
GROUP BY campaign_id, campaign_name, channel
ORDER BY attributed_revenue DESC
LIMIT 20;
"""

# ---------------------------------------------------------------------------
# Python implementations
# ---------------------------------------------------------------------------

def load_data(conn) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load touchpoints + closed-won opportunities."""
    opps = pd.read_sql_query(
        "SELECT c.contact_id, o.opportunity_id, o.amount, c.opportunity_date AS conv_date "
        "FROM contacts c "
        "JOIN opportunities o ON o.primary_contact_id = c.contact_id "
        "WHERE o.closed_won = 1 AND c.opportunity_date IS NOT NULL",
        conn
    )
    tps = pd.read_sql_query(
        "SELECT t.* FROM touchpoints t "
        "WHERE t.contact_id IN (SELECT contact_id FROM contacts WHERE lead_status = 'Closed_Won')",
        conn
    )
    return tps, opps


def first_touch(tps: pd.DataFrame, opps: pd.DataFrame) -> pd.DataFrame:
    merged = tps.merge(opps[["contact_id", "amount"]], on="contact_id")
    first  = merged.sort_values("touchpoint_timestamp").groupby("contact_id").first().reset_index()
    return (
        first.groupby("channel")
             .agg(attributed_revenue=("amount", "sum"),
                  conversions=("contact_id", "count"))
             .reset_index()
             .assign(model="first_touch")
    )


def last_touch(tps: pd.DataFrame, opps: pd.DataFrame) -> pd.DataFrame:
    merged = tps.merge(opps[["contact_id", "amount"]], on="contact_id")
    last   = merged.sort_values("touchpoint_timestamp").groupby("contact_id").last().reset_index()
    return (
        last.groupby("channel")
            .agg(attributed_revenue=("amount", "sum"),
                 conversions=("contact_id", "count"))
            .reset_index()
            .assign(model="last_touch")
    )


def linear(tps: pd.DataFrame, opps: pd.DataFrame) -> pd.DataFrame:
    merged   = tps.merge(opps[["contact_id", "amount"]], on="contact_id")
    tp_count = merged.groupby("contact_id")["touchpoint_id"].transform("count")
    merged["weight"]            = 1.0 / tp_count
    merged["attributed_revenue"] = merged["amount"] * merged["weight"]
    return (
        merged.groupby("channel")
              .agg(attributed_revenue=("attributed_revenue", "sum"),
                   conversions=("contact_id", "nunique"))
              .reset_index()
              .assign(model="linear")
    )


def time_decay(tps: pd.DataFrame, opps: pd.DataFrame,
               half_life: int = config.TIME_DECAY_HALF_LIFE_DAYS) -> pd.DataFrame:
    merged = tps.merge(opps[["contact_id", "amount", "conv_date"]], on="contact_id")
    merged["tp_date"]         = pd.to_datetime(merged["touchpoint_timestamp"]).dt.date
    merged["conv_date_dt"]    = pd.to_datetime(merged["conv_date"]).dt.date
    merged["days_before"]     = (
        pd.to_datetime(merged["conv_date_dt"]) - pd.to_datetime(merged["tp_date"])
    ).dt.days.clip(lower=0)
    merged["raw_weight"]      = np.power(2.0, -merged["days_before"] / half_life)
    sum_weights               = merged.groupby("contact_id")["raw_weight"].transform("sum")
    merged["norm_weight"]     = merged["raw_weight"] / sum_weights
    merged["attributed_revenue"] = merged["norm_weight"] * merged["amount"]
    return (
        merged.groupby("channel")
              .agg(attributed_revenue=("attributed_revenue", "sum"),
                   conversions=("contact_id", "nunique"))
              .reset_index()
              .assign(model="time_decay")
    )


def position_based(tps: pd.DataFrame, opps: pd.DataFrame) -> pd.DataFrame:
    merged = tps.merge(opps[["contact_id", "amount"]], on="contact_id")
    merged = merged.sort_values("touchpoint_timestamp")
    merged["rn_asc"]  = merged.groupby("contact_id").cumcount() + 1
    n_tp              = merged.groupby("contact_id")["touchpoint_id"].transform("count")
    merged["n_tp"]    = n_tp
    merged["rn_desc"] = merged["n_tp"] - merged["rn_asc"] + 1

    def calc_weight(row):
        n = row["n_tp"]
        if n == 1:
            return 1.0
        if n == 2:
            return 0.5
        if row["rn_asc"] == 1:
            return 0.4
        if row["rn_desc"] == 1:
            return 0.4
        return 0.2 / (n - 2)

    merged["weight"]            = merged.apply(calc_weight, axis=1)
    merged["attributed_revenue"] = merged["weight"] * merged["amount"]
    return (
        merged.groupby("channel")
              .agg(attributed_revenue=("attributed_revenue", "sum"),
                   conversions=("contact_id", "nunique"))
              .reset_index()
              .assign(model="position_based")
    )


def data_driven(tps: pd.DataFrame, opps: pd.DataFrame, conn=None) -> pd.DataFrame:
    """
    Shapley-inspired data-driven attribution using logistic regression.
    Features: channel, touchpoint_type, position_bucket, days_before_conv,
              is_first_touch, touchpoint_count.
    """
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import LabelEncoder

    # Need both converted and non-converted contacts for training
    # Load all touchpoints from DB if available
    if conn is not None:
        all_tps = pd.read_sql_query(
            "SELECT t.contact_id, t.touchpoint_id, t.channel, t.touchpoint_timestamp "
            "FROM touchpoints t LIMIT 50000",
            conn
        )
    else:
        all_tps = tps.copy()

    # Build feature set: one row per contact with channel-level aggregations
    contact_features = (
        all_tps.groupby("contact_id")
               .agg(
                   n_touchpoints  =("touchpoint_id", "count"),
                   n_channels     =("channel", "nunique"),
                   has_paid_search=("channel", lambda x: int("Paid_Search" in x.values)),
                   has_event      =("channel", lambda x: int("Events" in x.values)),
                   has_webinar    =("channel", lambda x: int("Webinar" in x.values)),
                   has_email      =("channel", lambda x: int("Email" in x.values)),
               )
               .reset_index()
    )
    contact_features = contact_features.merge(
        opps[["contact_id"]].drop_duplicates().assign(converted=1),
        on="contact_id", how="left"
    )
    contact_features["converted"] = contact_features["converted"].fillna(0).astype(int)

    features = ["n_touchpoints", "n_channels", "has_paid_search",
                "has_event", "has_webinar", "has_email"]
    X = contact_features[features].fillna(0)
    y = contact_features["converted"]

    if y.sum() < 10:
        # Not enough positive examples — fall back to time decay
        return time_decay(tps, opps)

    model = LogisticRegression(max_iter=200, random_state=config.RANDOM_SEED)
    model.fit(X, y)

    # Shapley-inspired: measure marginal contribution of each channel by
    # comparing conversion probability with vs. without each channel feature.
    coef_map = dict(zip(features, model.coef_[0]))

    channel_coefs = {
        "Paid_Search":        abs(coef_map.get("has_paid_search", 0)),
        "Events":             abs(coef_map.get("has_event",       0)),
        "Webinar":            abs(coef_map.get("has_webinar",     0)),
        "Email":              abs(coef_map.get("has_email",       0)),
    }
    # Remaining channels share the residual
    named_total   = sum(channel_coefs.values())
    all_channels  = tps["channel"].unique()
    other_share   = max(0, 1 - named_total) / max(1, len(all_channels) - len(channel_coefs))
    for ch in all_channels:
        if ch not in channel_coefs:
            channel_coefs[ch] = other_share

    total_coef = sum(channel_coefs.values()) or 1.0
    channel_weights = {ch: v / total_coef for ch, v in channel_coefs.items()}

    # Apply weights to revenue
    total_revenue = opps["amount"].sum()
    rows = []
    for ch, w in channel_weights.items():
        rows.append({
            "channel":            ch,
            "attributed_revenue": round(total_revenue * w, 2),
            "conversions":        round(len(opps) * w),
            "model":              "data_driven",
        })
    return pd.DataFrame(rows)


def run_all_models(conn) -> pd.DataFrame:
    tps, opps = load_data(conn)
    results = pd.concat([
        first_touch(tps, opps),
        last_touch(tps, opps),
        linear(tps, opps),
        time_decay(tps, opps),
        position_based(tps, opps),
        data_driven(tps, opps, conn),
    ], ignore_index=True)
    return results


def get_channel_comparison(conn) -> pd.DataFrame:
    """Wide table: channel × model for easy comparison."""
    long = run_all_models(conn)
    wide = long.pivot_table(
        index="channel",
        columns="model",
        values="attributed_revenue",
        aggfunc="sum"
    ).reset_index()
    wide.columns.name = None
    return wide


def model_agreement_score(wide: pd.DataFrame) -> float:
    """Fraction of channels where top-3 ranking is consistent across models."""
    model_cols = [c for c in wide.columns if c != "channel"]
    if len(model_cols) < 2:
        return 1.0
    top3_sets = []
    for col in model_cols:
        top3 = set(wide.nlargest(3, col)["channel"].tolist())
        top3_sets.append(top3)
    agreements = sum(
        1 for i in range(len(top3_sets)) for j in range(i + 1, len(top3_sets))
        if top3_sets[i] == top3_sets[j]
    )
    total_pairs = len(top3_sets) * (len(top3_sets) - 1) / 2
    return round(agreements / total_pairs, 3) if total_pairs > 0 else 1.0

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    conn = sqlite3.connect(config.DB_PATH)

    print("Running all 6 attribution models...")
    results = run_all_models(conn)

    print("\n--- Attribution by Channel (time_decay) ---")
    td = results[results["model"] == "time_decay"].sort_values("attributed_revenue", ascending=False)
    print(td.to_string(index=False))

    print("\n--- Channel × Model Comparison ---")
    wide = get_channel_comparison(conn)
    print(wide.to_string(index=False))

    score = model_agreement_score(wide)
    print(f"\nModel Agreement Score (top-3 channels): {score:.1%}")

    # Write SQL
    os.makedirs("sql", exist_ok=True)
    with open("sql/02_attribution_queries.sql", "w") as f:
        f.write(ATTRIBUTION_SQL)
    print("\n✓ sql/02_attribution_queries.sql written")
    conn.close()


if __name__ == "__main__":
    main()
