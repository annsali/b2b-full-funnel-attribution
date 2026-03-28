"""
Revenue Attribution — maps closed-won revenue back to channels and campaigns
across all 6 attribution models.

Run: python -m src.revenue_attribution
"""
import os, sys, sqlite3
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from src.attribution_models import (
    load_data, first_touch, last_touch, linear,
    time_decay, position_based, data_driven,
    model_agreement_score,
)

REVENUE_SQL = """
-- ==========================================================================
-- 05_revenue_attribution.sql
-- Revenue mapped back to channels, campaigns, and content assets.
-- ==========================================================================

-- --------------------------------------------------------------------------
-- Query 1: Revenue by Channel Across All Attribution Models
-- Uses time_decay as primary; other models shown for comparison.
-- --------------------------------------------------------------------------
WITH contact_opps AS (
    SELECT c.contact_id, o.amount, c.opportunity_date AS conv_date
    FROM contacts c
    JOIN opportunities o ON o.primary_contact_id = c.contact_id
    WHERE o.closed_won = 1
),
tp_base AS (
    SELECT t.touchpoint_id, t.contact_id, t.channel, t.cost, co.amount, co.conv_date,
           ROW_NUMBER() OVER (PARTITION BY t.contact_id ORDER BY t.touchpoint_timestamp)         AS rn_asc,
           ROW_NUMBER() OVER (PARTITION BY t.contact_id ORDER BY t.touchpoint_timestamp DESC)    AS rn_desc,
           COUNT(*)     OVER (PARTITION BY t.contact_id)                                          AS n_tp,
           POWER(2.0, -(JULIANDAY(co.conv_date) -
                        JULIANDAY(SUBSTR(t.touchpoint_timestamp,1,10))) / 7.0) AS raw_decay
    FROM touchpoints t
    JOIN contact_opps co ON co.contact_id = t.contact_id
),
norm AS (
    SELECT *,
           raw_decay / SUM(raw_decay) OVER (PARTITION BY contact_id) AS decay_w
    FROM tp_base
)
SELECT
    channel,
    ROUND(SUM(CASE WHEN rn_asc=1  THEN amount ELSE 0 END), 0) AS first_touch_rev,
    ROUND(SUM(CASE WHEN rn_desc=1 THEN amount ELSE 0 END), 0) AS last_touch_rev,
    ROUND(SUM(amount / n_tp), 0)                               AS linear_rev,
    ROUND(SUM(decay_w * amount), 0)                            AS time_decay_rev,
    ROUND(SUM(CASE
        WHEN n_tp=1  THEN 1.0
        WHEN n_tp=2  THEN 0.5
        WHEN rn_asc=1  THEN 0.4
        WHEN rn_desc=1 THEN 0.4
        ELSE 0.2/(n_tp-2.0) END * amount), 0)                 AS position_rev,
    ROUND(SUM(COALESCE(cost, 0)), 0)                           AS total_spend,
    ROUND(SUM(decay_w * amount) / NULLIF(SUM(COALESCE(cost, 0)), 0), 2) AS roas_time_decay
FROM norm
GROUP BY channel
ORDER BY time_decay_rev DESC;

-- --------------------------------------------------------------------------
-- Query 2: Top Campaigns by Attributed Revenue (Time Decay model)
-- --------------------------------------------------------------------------
WITH contact_opps AS (
    SELECT c.contact_id, o.amount, c.opportunity_date AS conv_date
    FROM contacts c
    JOIN opportunities o ON o.primary_contact_id = c.contact_id
    WHERE o.closed_won = 1
),
td AS (
    SELECT t.campaign_id, t.campaign_name, t.channel, t.contact_id, t.cost, co.amount,
           POWER(2.0, -(JULIANDAY(co.conv_date) -
                        JULIANDAY(SUBSTR(t.touchpoint_timestamp,1,10))) / 7.0) AS raw_decay
    FROM touchpoints t
    JOIN contact_opps co ON co.contact_id = t.contact_id
),
norm AS (
    SELECT *,
           raw_decay / SUM(raw_decay) OVER (PARTITION BY contact_id) AS decay_w,
           raw_decay / SUM(raw_decay) OVER (PARTITION BY contact_id) * amount AS td_rev
    FROM td
)
SELECT
    campaign_id, campaign_name, channel,
    ROUND(SUM(td_rev), 0)                                                   AS attributed_revenue,
    ROUND(SUM(COALESCE(cost, 0)), 0)                                        AS campaign_cost,
    COUNT(DISTINCT contact_id)                                              AS attributed_contacts,
    ROUND(SUM(td_rev) / NULLIF(SUM(COALESCE(cost, 0)), 0), 2)              AS roi,
    RANK() OVER (ORDER BY SUM(td_rev) DESC)                                 AS rank
FROM norm
GROUP BY campaign_id, campaign_name, channel
ORDER BY attributed_revenue DESC
LIMIT 15;

-- --------------------------------------------------------------------------
-- Query 3: Content Asset Attribution
-- Which whitepapers and webinars drive the most revenue?
-- --------------------------------------------------------------------------
WITH contact_opps AS (
    SELECT c.contact_id, o.amount, c.opportunity_date AS conv_date
    FROM contacts c
    JOIN opportunities o ON o.primary_contact_id = c.contact_id
    WHERE o.closed_won = 1
),
td AS (
    SELECT t.content_asset, t.contact_id, co.amount,
           POWER(2.0, -(JULIANDAY(co.conv_date) -
                        JULIANDAY(SUBSTR(t.touchpoint_timestamp,1,10))) / 7.0) AS raw_decay
    FROM touchpoints t
    JOIN contact_opps co ON co.contact_id = t.contact_id
    WHERE t.content_asset IS NOT NULL
),
norm AS (
    SELECT *,
           raw_decay / SUM(raw_decay) OVER (PARTITION BY contact_id) AS decay_w,
           raw_decay / SUM(raw_decay) OVER (PARTITION BY contact_id) * amount AS td_rev
    FROM td
)
SELECT
    content_asset,
    ROUND(SUM(td_rev), 0)           AS attributed_revenue,
    COUNT(DISTINCT contact_id)       AS contacts_influenced,
    RANK() OVER (ORDER BY SUM(td_rev) DESC) AS rank
FROM norm
GROUP BY content_asset
ORDER BY attributed_revenue DESC;

-- --------------------------------------------------------------------------
-- Query 4: Monthly Revenue Attribution Trend (Time Decay)
-- --------------------------------------------------------------------------
WITH contact_opps AS (
    SELECT c.contact_id, o.amount, c.closed_date AS close_month_raw,
           SUBSTR(c.closed_date, 1, 7) AS close_month
    FROM contacts c
    JOIN opportunities o ON o.primary_contact_id = c.contact_id
    WHERE o.closed_won = 1 AND c.closed_date IS NOT NULL
),
td AS (
    SELECT t.channel, co.close_month, co.amount, co.contact_id,
           POWER(2.0, -(JULIANDAY(co.close_month_raw) -
                        JULIANDAY(SUBSTR(t.touchpoint_timestamp,1,10))) / 7.0) AS raw_decay
    FROM touchpoints t
    JOIN contact_opps co ON co.contact_id = t.contact_id
),
norm AS (
    SELECT *,
           raw_decay / SUM(raw_decay) OVER (PARTITION BY contact_id) AS decay_w,
           raw_decay / SUM(raw_decay) OVER (PARTITION BY contact_id) * amount AS td_rev
    FROM td
)
SELECT
    close_month,
    channel,
    ROUND(SUM(td_rev), 0) AS attributed_revenue
FROM norm
GROUP BY close_month, channel
ORDER BY close_month, attributed_revenue DESC;
"""

# ---------------------------------------------------------------------------
# Python revenue attribution
# ---------------------------------------------------------------------------

def build_channel_revenue_table(conn) -> pd.DataFrame:
    """All 6 models, channel level, with cost and ROAS."""
    tps, opps = load_data(conn)

    ft  = first_touch(tps, opps).rename(columns={"attributed_revenue": "first_touch_rev"})
    lt  = last_touch(tps, opps).rename(columns={"attributed_revenue": "last_touch_rev"})
    lin = linear(tps, opps).rename(columns={"attributed_revenue": "linear_rev"})
    td  = time_decay(tps, opps).rename(columns={"attributed_revenue": "time_decay_rev"})
    pb  = position_based(tps, opps).rename(columns={"attributed_revenue": "position_rev"})
    dd  = data_driven(tps, opps, conn).rename(columns={"attributed_revenue": "data_driven_rev"})

    # Channel costs from touchpoints
    cost_sql = """
    SELECT channel, ROUND(SUM(COALESCE(cost,0)),0) AS total_spend
    FROM touchpoints GROUP BY channel
    """
    costs = pd.read_sql_query(cost_sql, conn)

    result = (
        ft[["channel", "first_touch_rev"]]
          .merge(lt[["channel", "last_touch_rev"]],   on="channel", how="outer")
          .merge(lin[["channel", "linear_rev"]],       on="channel", how="outer")
          .merge(td[["channel", "time_decay_rev"]],    on="channel", how="outer")
          .merge(pb[["channel", "position_rev"]],      on="channel", how="outer")
          .merge(dd[["channel", "data_driven_rev"]],   on="channel", how="outer")
          .merge(costs,                                on="channel", how="left")
    )
    result["roas"] = (result["time_decay_rev"] / result["total_spend"].replace(0, np.nan)).round(2)
    total = result["time_decay_rev"].sum()
    result["pct_of_total_revenue"] = (result["time_decay_rev"] / total).round(4)
    return result.sort_values("time_decay_rev", ascending=False)


def build_campaign_revenue_table(conn) -> pd.DataFrame:
    sql = """
    WITH co AS (
        SELECT c.contact_id, o.amount, c.opportunity_date AS conv_date
        FROM contacts c
        JOIN opportunities o ON o.primary_contact_id=c.contact_id
        WHERE o.closed_won=1
    ),
    td AS (
        SELECT t.campaign_id, t.campaign_name, t.channel, t.contact_id, t.cost, co.amount,
               POWER(2.0,-(JULIANDAY(co.conv_date)-JULIANDAY(SUBSTR(t.touchpoint_timestamp,1,10)))/7.0) AS rd
        FROM touchpoints t JOIN co ON co.contact_id=t.contact_id
    ),
    norm AS (
        SELECT *, rd/SUM(rd) OVER (PARTITION BY contact_id)*amount AS td_rev
        FROM td
    )
    SELECT campaign_id, campaign_name, channel,
           ROUND(SUM(td_rev),0) AS attributed_revenue,
           ROUND(SUM(COALESCE(cost,0)),0) AS campaign_cost,
           COUNT(DISTINCT contact_id) AS contacts,
           ROUND(SUM(td_rev)/NULLIF(SUM(COALESCE(cost,0)),0),2) AS roi
    FROM norm
    GROUP BY campaign_id, campaign_name, channel
    ORDER BY attributed_revenue DESC
    """
    return pd.read_sql_query(sql, conn)


def build_content_attribution(conn) -> pd.DataFrame:
    sql = """
    WITH co AS (
        SELECT c.contact_id, o.amount, c.opportunity_date AS conv_date
        FROM contacts c
        JOIN opportunities o ON o.primary_contact_id=c.contact_id
        WHERE o.closed_won=1
    ),
    td AS (
        SELECT t.content_asset, t.contact_id, co.amount,
               POWER(2.0,-(JULIANDAY(co.conv_date)-JULIANDAY(SUBSTR(t.touchpoint_timestamp,1,10)))/7.0) AS rd
        FROM touchpoints t JOIN co ON co.contact_id=t.contact_id
        WHERE t.content_asset IS NOT NULL
    ),
    norm AS (
        SELECT *, rd/SUM(rd) OVER (PARTITION BY contact_id)*amount AS td_rev
        FROM td
    )
    SELECT content_asset,
           ROUND(SUM(td_rev),0) AS attributed_revenue,
           COUNT(DISTINCT contact_id) AS contacts_influenced
    FROM norm GROUP BY content_asset ORDER BY attributed_revenue DESC
    """
    return pd.read_sql_query(sql, conn)


def run_all(conn=None) -> dict:
    if conn is None:
        conn = sqlite3.connect(config.DB_PATH)
    return {
        "channel_revenue":  build_channel_revenue_table(conn),
        "campaign_revenue": build_campaign_revenue_table(conn),
        "content_revenue":  build_content_attribution(conn),
    }

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    conn = sqlite3.connect(config.DB_PATH)

    print("Running revenue attribution...")
    results = run_all(conn)

    print("\n--- Channel Revenue (all models) ---")
    print(results["channel_revenue"].to_string(index=False))

    print("\n--- Top 5 Campaigns ---")
    print(results["campaign_revenue"].head(5).to_string(index=False))

    print("\n--- Content Assets ---")
    print(results["content_revenue"].to_string(index=False))

    os.makedirs("sql", exist_ok=True)
    with open("sql/05_revenue_attribution.sql", "w") as f:
        f.write(REVENUE_SQL)
    print("\n✓ sql/05_revenue_attribution.sql written")
    conn.close()


if __name__ == "__main__":
    main()
