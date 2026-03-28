"""
Lead Velocity Metrics — pipeline speed and momentum.

Metrics:
    - Lead Velocity Rate (LVR): MoM growth in qualified leads
    - Pipeline Velocity: (Opps × Win_Rate × Avg_Deal) / Avg_Sales_Cycle
    - Stage Velocity: avg days per stage transition
    - Acceleration Index: recent vs historical velocity
    - Time-to-Revenue: first touch → closed-won
    - Marketing Influenced vs Sourced pipeline

Run: python -m src.lead_velocity
"""
import os, sys, sqlite3
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

VELOCITY_SQL = """
-- ==========================================================================
-- 03_lead_velocity.sql
-- Pipeline speed and momentum metrics.
-- ==========================================================================

-- --------------------------------------------------------------------------
-- Query 1: Lead Velocity Rate (LVR) Trending
-- MoM growth in MQL, SQL, Opp, and Closed Won counts.
-- --------------------------------------------------------------------------
WITH monthly AS (
    SELECT
        SUBSTR(created_date, 1, 7)                                   AS month,
        COUNT(*) AS new_leads,
        COUNT(CASE WHEN mql_date IS NOT NULL THEN 1 END)              AS mqls,
        COUNT(CASE WHEN sql_date IS NOT NULL THEN 1 END)              AS sqls,
        COUNT(CASE WHEN opportunity_date IS NOT NULL THEN 1 END)      AS opps,
        COUNT(CASE WHEN lead_status = 'Closed_Won' THEN 1 END)        AS won
    FROM contacts
    GROUP BY SUBSTR(created_date, 1, 7)
),
with_lag AS (
    SELECT *,
           LAG(mqls, 1) OVER (ORDER BY month) AS prev_mqls,
           LAG(opps, 1) OVER (ORDER BY month) AS prev_opps
    FROM monthly
)
SELECT
    month,
    new_leads, mqls, sqls, opps, won,
    ROUND(100.0 * (mqls - prev_mqls) / NULLIF(prev_mqls, 0), 1) AS lvr_pct,
    ROUND(100.0 * (opps - prev_opps) / NULLIF(prev_opps, 0), 1) AS pipeline_growth_pct
FROM with_lag
ORDER BY month;

-- --------------------------------------------------------------------------
-- Query 2: Pipeline Velocity by Segment
-- Formula: (Opps × Win_Rate × Avg_Deal_Size) / Avg_Sales_Cycle_Days
-- --------------------------------------------------------------------------
WITH opp_metrics AS (
    SELECT
        a.company_size,
        a.industry,
        o.product_line,
        COUNT(o.opportunity_id)                                      AS n_opps,
        AVG(CASE WHEN o.closed_won = 1 THEN 1.0 ELSE 0.0 END)       AS win_rate,
        AVG(o.amount)                                                AS avg_deal,
        AVG(o.days_to_close)                                         AS avg_days
    FROM opportunities o
    JOIN accounts a ON a.account_id = o.account_id
    GROUP BY a.company_size, a.industry, o.product_line
)
SELECT
    company_size,
    industry,
    product_line,
    n_opps,
    ROUND(win_rate, 3)                                               AS win_rate,
    ROUND(avg_deal, 0)                                               AS avg_deal,
    ROUND(avg_days, 1)                                               AS avg_days_to_close,
    ROUND((n_opps * win_rate * avg_deal) / NULLIF(avg_days, 0), 0)  AS pipeline_velocity
FROM opp_metrics
ORDER BY pipeline_velocity DESC;

-- --------------------------------------------------------------------------
-- Query 3: Stage Transition Matrix
-- For each consecutive stage pair: volume, conversion rate, avg days.
-- --------------------------------------------------------------------------
WITH stage_pairs AS (
    SELECT
        s1.stage_name            AS from_stage,
        s2.stage_name            AS to_stage,
        COUNT(*)                 AS transitions,
        AVG(s1.days_in_stage)    AS avg_days_in_from_stage,
        -- median approximation
        MIN(CASE WHEN s1.days_in_stage IS NOT NULL THEN s1.days_in_stage END) AS min_days
    FROM lead_stages s1
    JOIN lead_stages s2
      ON s1.contact_id = s2.contact_id
     AND s2.entered_date > s1.entered_date
     AND NOT EXISTS (
         SELECT 1 FROM lead_stages s3
         WHERE s3.contact_id = s1.contact_id
           AND s3.entered_date > s1.entered_date
           AND s3.entered_date < s2.entered_date
     )
    WHERE s1.stage_name IN ('New_Lead','MQL','SQL','SAL','Opportunity','Negotiation')
    GROUP BY s1.stage_name, s2.stage_name
)
SELECT * FROM stage_pairs ORDER BY transitions DESC;

-- --------------------------------------------------------------------------
-- Query 4: Time-to-Revenue by Channel (first touch → closed-won)
-- --------------------------------------------------------------------------
WITH first_touches AS (
    SELECT
        t.contact_id,
        t.channel,
        MIN(t.touchpoint_timestamp) AS first_touch_ts
    FROM touchpoints t
    GROUP BY t.contact_id, t.channel
    HAVING ROW_NUMBER() OVER (PARTITION BY t.contact_id ORDER BY MIN(t.touchpoint_timestamp)) = 1
),
time_to_rev AS (
    SELECT
        ft.channel,
        JULIANDAY(c.closed_date) - JULIANDAY(SUBSTR(ft.first_touch_ts,1,10)) AS days_to_revenue
    FROM first_touches ft
    JOIN contacts c ON c.contact_id = ft.contact_id
    WHERE c.lead_status = 'Closed_Won'
      AND c.closed_date IS NOT NULL
)
SELECT
    channel,
    COUNT(*)                            AS closed_won_count,
    ROUND(AVG(days_to_revenue),   1)    AS avg_days_to_revenue,
    ROUND(MIN(days_to_revenue),   1)    AS min_days,
    ROUND(MAX(days_to_revenue),   1)    AS max_days
FROM time_to_rev
GROUP BY channel
ORDER BY avg_days_to_revenue;

-- --------------------------------------------------------------------------
-- Query 5: Marketing Sourced vs Influenced Pipeline
-- --------------------------------------------------------------------------
WITH first_touch_channel AS (
    SELECT contact_id, channel AS first_touch_channel
    FROM (
        SELECT contact_id, channel,
               ROW_NUMBER() OVER (PARTITION BY contact_id ORDER BY touchpoint_timestamp) AS rn
        FROM touchpoints
    )
    WHERE rn = 1
),
marketing_channels AS (
    SELECT contact_id
    FROM touchpoints
    WHERE channel NOT IN ('Outbound_Sales')
    GROUP BY contact_id
),
opp_data AS (
    SELECT o.opportunity_id, o.account_id, o.primary_contact_id, o.amount, o.closed_won
    FROM opportunities o
)
SELECT
    CASE
        WHEN ftc.first_touch_channel NOT IN ('Outbound_Sales') THEN 'Marketing_Sourced'
        WHEN mc.contact_id IS NOT NULL                         THEN 'Marketing_Influenced'
        ELSE 'Sales_Only'
    END AS attribution_type,
    COUNT(od.opportunity_id)                                    AS opportunity_count,
    ROUND(SUM(od.amount), 0)                                    AS total_pipeline,
    ROUND(AVG(od.amount), 0)                                    AS avg_deal_size,
    ROUND(AVG(CAST(od.closed_won AS FLOAT)), 3)                 AS win_rate
FROM opp_data od
LEFT JOIN first_touch_channel ftc ON ftc.contact_id = od.primary_contact_id
LEFT JOIN marketing_channels   mc ON mc.contact_id  = od.primary_contact_id
GROUP BY attribution_type
ORDER BY total_pipeline DESC;
"""

# ---------------------------------------------------------------------------
# Python metrics
# ---------------------------------------------------------------------------

def compute_lvr(conn) -> pd.DataFrame:
    """Lead Velocity Rate: MoM growth at each funnel stage."""
    sql = """
    WITH monthly AS (
        SELECT SUBSTR(created_date,1,7) AS month,
               COUNT(*) AS new_leads,
               COUNT(CASE WHEN mql_date IS NOT NULL THEN 1 END) AS mqls,
               COUNT(CASE WHEN sql_date IS NOT NULL THEN 1 END) AS sqls,
               COUNT(CASE WHEN opportunity_date IS NOT NULL THEN 1 END) AS opps,
               COUNT(CASE WHEN lead_status='Closed_Won' THEN 1 END) AS won
        FROM contacts GROUP BY SUBSTR(created_date,1,7)
    )
    SELECT month, new_leads, mqls, sqls, opps, won FROM monthly ORDER BY month
    """
    df = pd.read_sql_query(sql, conn)
    for col in ["mqls", "sqls", "opps", "won"]:
        df[f"{col}_lvr_pct"] = df[col].pct_change() * 100
    return df


def compute_pipeline_velocity(conn) -> pd.DataFrame:
    """(Opps × Win_Rate × Avg_Deal) / Avg_Sales_Cycle by segment."""
    sql = """
    SELECT a.company_size, a.industry, o.product_line,
           COUNT(o.opportunity_id) AS n_opps,
           AVG(CASE WHEN o.closed_won=1 THEN 1.0 ELSE 0.0 END) AS win_rate,
           AVG(o.amount) AS avg_deal,
           AVG(o.days_to_close) AS avg_days
    FROM opportunities o
    JOIN accounts a ON a.account_id=o.account_id
    GROUP BY a.company_size, a.industry, o.product_line
    """
    df = pd.read_sql_query(sql, conn)
    df["pipeline_velocity"] = (
        df["n_opps"] * df["win_rate"] * df["avg_deal"]
    ) / df["avg_days"].replace(0, np.nan)
    return df.sort_values("pipeline_velocity", ascending=False)


def compute_stage_durations(conn) -> pd.DataFrame:
    sql = """
    SELECT stage_name, days_in_stage FROM lead_stages
    WHERE days_in_stage >= 0 AND days_in_stage IS NOT NULL
      AND stage_name IN ('New_Lead','MQL','SQL','SAL','Opportunity','Negotiation')
    """
    df = pd.read_sql_query(sql, conn)
    return (
        df.groupby("stage_name")["days_in_stage"]
          .agg(["mean", "median", lambda x: x.quantile(0.75), lambda x: x.quantile(0.90)])
          .rename(columns={"mean": "avg_days", "median": "median_days",
                           "<lambda_0>": "p75_days", "<lambda_1>": "p90_days"})
          .reset_index()
    )


def compute_acceleration_index(conn) -> pd.DataFrame:
    """Ratio of last-30-day MQL rate to 90-day historical rate."""
    sql = """
    SELECT
        ROUND(1.0 * SUM(CASE WHEN mql_date >= DATE('now', '-30 days') THEN 1 ELSE 0 END)
              / NULLIF(SUM(CASE WHEN mql_date >= DATE('now', '-90 days')
                                 AND mql_date < DATE('now', '-30 days') THEN 1 ELSE 0 END), 0)
              * 3.0, 3) AS acceleration_index
    FROM contacts
    WHERE mql_date IS NOT NULL
    """
    return pd.read_sql_query(sql, conn)


def compute_time_to_revenue(conn) -> pd.DataFrame:
    sql = """
    WITH first_tp AS (
        SELECT contact_id, MIN(touchpoint_timestamp) AS first_ts
        FROM touchpoints GROUP BY contact_id
    )
    SELECT
        ROUND(AVG(JULIANDAY(c.closed_date) - JULIANDAY(SUBSTR(ftp.first_ts,1,10))), 1)
            AS avg_days_first_touch_to_revenue,
        COUNT(*) AS n
    FROM contacts c
    JOIN first_tp ftp ON ftp.contact_id = c.contact_id
    WHERE c.lead_status = 'Closed_Won' AND c.closed_date IS NOT NULL
    """
    return pd.read_sql_query(sql, conn)


def compute_marketing_pipeline(conn) -> pd.DataFrame:
    sql = """
    WITH mkt_contacts AS (
        SELECT DISTINCT contact_id FROM touchpoints
        WHERE channel NOT IN ('Outbound_Sales')
    ),
    first_touch AS (
        SELECT contact_id,
               channel AS first_channel
        FROM (
            SELECT contact_id, channel,
                   ROW_NUMBER() OVER (PARTITION BY contact_id ORDER BY touchpoint_timestamp) AS rn
            FROM touchpoints
        ) WHERE rn = 1
    )
    SELECT
        CASE WHEN ft.first_channel NOT IN ('Outbound_Sales') THEN 'Marketing_Sourced'
             WHEN mc.contact_id IS NOT NULL THEN 'Marketing_Influenced'
             ELSE 'Sales_Only' END AS type,
        COUNT(o.opportunity_id) AS opps,
        ROUND(SUM(o.amount), 0) AS pipeline_value,
        ROUND(AVG(CAST(o.closed_won AS FLOAT)), 3) AS win_rate
    FROM opportunities o
    LEFT JOIN first_touch ft ON ft.contact_id = o.primary_contact_id
    LEFT JOIN mkt_contacts mc ON mc.contact_id = o.primary_contact_id
    GROUP BY type
    """
    return pd.read_sql_query(sql, conn)


def run_all(conn=None) -> dict:
    if conn is None:
        conn = sqlite3.connect(config.DB_PATH)
    return {
        "lvr":                  compute_lvr(conn),
        "pipeline_velocity":    compute_pipeline_velocity(conn),
        "stage_durations":      compute_stage_durations(conn),
        "time_to_revenue":      compute_time_to_revenue(conn),
        "marketing_pipeline":   compute_marketing_pipeline(conn),
    }

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    conn = sqlite3.connect(config.DB_PATH)

    print("Computing lead velocity metrics...")
    results = run_all(conn)

    print("\n--- Lead Velocity Rate (last 6 months) ---")
    lvr = results["lvr"].tail(6)
    print(lvr[["month", "mqls", "mqls_lvr_pct", "opps", "opps_lvr_pct"]].to_string(index=False))

    print("\n--- Time to Revenue ---")
    print(results["time_to_revenue"].to_string(index=False))

    print("\n--- Marketing Pipeline ---")
    print(results["marketing_pipeline"].to_string(index=False))

    os.makedirs("sql", exist_ok=True)
    with open("sql/03_lead_velocity.sql", "w") as f:
        f.write(VELOCITY_SQL)
    print("\n✓ sql/03_lead_velocity.sql written")
    conn.close()


if __name__ == "__main__":
    main()
