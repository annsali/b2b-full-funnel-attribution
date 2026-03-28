"""
Funnel Engine — full-funnel conversion metrics via SQL.

Run: python -m src.funnel_engine
"""
import os, sys, sqlite3
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

# ---------------------------------------------------------------------------
# SQL query library (also written to sql/01_funnel_metrics.sql)
# ---------------------------------------------------------------------------

FUNNEL_METRICS_SQL = """
-- ==========================================================================
-- 01_funnel_metrics.sql
-- Full-funnel conversion rates, stage durations, and velocity over time.
-- Context: B2B Marketing funnel tracking 90K contacts across Lead → MQL →
--          SQL → SAL → Opportunity → Closed Won/Lost.
-- ==========================================================================

-- --------------------------------------------------------------------------
-- Query 1: Full-Funnel Conversion Waterfall
-- Shows stage-by-stage conversion rates with cumulative drop-off from top.
-- Breaks out by: overall, channel, industry, company_size.
-- --------------------------------------------------------------------------
WITH stage_counts AS (
    SELECT
        COUNT(*)                                              AS total_leads,
        COUNT(CASE WHEN lead_status != 'New_Lead'
                    AND mql_date IS NOT NULL  THEN 1 END)    AS total_mql,
        COUNT(CASE WHEN sql_date  IS NOT NULL THEN 1 END)    AS total_sql,
        COUNT(CASE WHEN sal_date  IS NOT NULL THEN 1 END)    AS total_sal,
        COUNT(CASE WHEN opportunity_date IS NOT NULL THEN 1 END) AS total_opp,
        COUNT(CASE WHEN lead_status = 'Closed_Won' THEN 1 END)   AS total_won
    FROM contacts
),
rates AS (
    SELECT
        total_leads,
        total_mql,
        total_sql,
        total_sal,
        total_opp,
        total_won,
        ROUND(1.0 * total_mql / NULLIF(total_leads, 0), 4) AS lead_to_mql,
        ROUND(1.0 * total_sql / NULLIF(total_mql,   0), 4) AS mql_to_sql,
        ROUND(1.0 * total_sal / NULLIF(total_sql,   0), 4) AS sql_to_sal,
        ROUND(1.0 * total_opp / NULLIF(total_sal,   0), 4) AS sal_to_opp,
        ROUND(1.0 * total_won / NULLIF(total_opp,   0), 4) AS opp_to_won,
        ROUND(1.0 * total_won / NULLIF(total_leads, 0), 4) AS cumulative_top_to_won
    FROM stage_counts
)
SELECT * FROM rates;

-- Conversion by lead_source
WITH source_funnel AS (
    SELECT
        c.lead_source,
        COUNT(*)                                                  AS leads,
        COUNT(CASE WHEN c.mql_date IS NOT NULL THEN 1 END)        AS mql,
        COUNT(CASE WHEN c.sql_date IS NOT NULL THEN 1 END)        AS sql_cnt,
        COUNT(CASE WHEN c.sal_date IS NOT NULL THEN 1 END)        AS sal,
        COUNT(CASE WHEN c.opportunity_date IS NOT NULL THEN 1 END) AS opp,
        COUNT(CASE WHEN c.lead_status = 'Closed_Won' THEN 1 END)  AS won,
        AVG(CASE WHEN o.closed_won = 1 THEN o.amount END)         AS avg_deal_size,
        SUM(CASE WHEN o.closed_won = 1 THEN o.amount ELSE 0 END)  AS total_revenue
    FROM contacts c
    LEFT JOIN opportunities o ON o.primary_contact_id = c.contact_id
    GROUP BY c.lead_source
)
SELECT
    lead_source,
    leads,
    mql,
    sql_cnt  AS sql_count,
    sal,
    opp,
    won,
    ROUND(1.0 * mql  / NULLIF(leads, 0), 4) AS lead_to_mql_rate,
    ROUND(1.0 * sql_cnt / NULLIF(mql, 0), 4) AS mql_to_sql_rate,
    ROUND(1.0 * won  / NULLIF(opp,  0), 4) AS win_rate,
    ROUND(avg_deal_size, 0)                  AS avg_deal_size,
    ROUND(total_revenue, 0)                  AS total_revenue,
    RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
FROM source_funnel
ORDER BY total_revenue DESC;

-- --------------------------------------------------------------------------
-- Query 2: Stage Duration Analysis (avg / median / P75 / P90)
-- --------------------------------------------------------------------------
WITH stage_durations AS (
    SELECT
        stage_name,
        days_in_stage,
        NTILE(4) OVER (PARTITION BY stage_name ORDER BY days_in_stage) AS quartile,
        NTILE(10) OVER (PARTITION BY stage_name ORDER BY days_in_stage) AS decile,
        COUNT(*) OVER (PARTITION BY stage_name)                          AS stage_n
    FROM lead_stages
    WHERE days_in_stage IS NOT NULL
      AND days_in_stage >= 0
      AND stage_name IN ('MQL','SQL','SAL','Opportunity','Negotiation')
),
agg AS (
    SELECT
        stage_name,
        COUNT(*)                              AS n,
        ROUND(AVG(days_in_stage), 1)          AS avg_days,
        MIN(CASE WHEN decile = 5 THEN days_in_stage END) AS median_days,
        MIN(CASE WHEN quartile = 3 THEN days_in_stage END) AS p75_days,
        MIN(CASE WHEN decile  = 9 THEN days_in_stage END) AS p90_days
    FROM stage_durations
    GROUP BY stage_name
)
SELECT
    stage_name,
    n,
    avg_days,
    median_days,
    p75_days,
    p90_days,
    CASE
        WHEN stage_name = 'MQL'         AND median_days > 25 THEN 'BOTTLENECK'
        WHEN stage_name = 'SQL'         AND median_days > 14 THEN 'BOTTLENECK'
        WHEN stage_name = 'SAL'         AND median_days > 20 THEN 'BOTTLENECK'
        WHEN stage_name = 'Opportunity' AND median_days > 60 THEN 'BOTTLENECK'
        ELSE 'OK'
    END AS bottleneck_flag
FROM agg
ORDER BY CASE stage_name
    WHEN 'MQL'         THEN 1
    WHEN 'SQL'         THEN 2
    WHEN 'SAL'         THEN 3
    WHEN 'Opportunity' THEN 4
    WHEN 'Negotiation' THEN 5
END;

-- --------------------------------------------------------------------------
-- Query 3: Funnel Velocity Over Time (monthly MoM growth)
-- --------------------------------------------------------------------------
WITH monthly AS (
    SELECT
        SUBSTR(created_date, 1, 7)          AS month,
        COUNT(*)                             AS new_leads,
        COUNT(CASE WHEN mql_date  IS NOT NULL THEN 1 END) AS mqls,
        COUNT(CASE WHEN sql_date  IS NOT NULL THEN 1 END) AS sqls,
        COUNT(CASE WHEN opportunity_date IS NOT NULL THEN 1 END) AS opps,
        COUNT(CASE WHEN lead_status = 'Closed_Won' THEN 1 END)   AS closed_won
    FROM contacts
    GROUP BY SUBSTR(created_date, 1, 7)
),
with_lag AS (
    SELECT
        month,
        new_leads,
        mqls,
        sqls,
        opps,
        closed_won,
        LAG(mqls,  1) OVER (ORDER BY month) AS prev_mqls,
        LAG(sqls,  1) OVER (ORDER BY month) AS prev_sqls,
        LAG(opps,  1) OVER (ORDER BY month) AS prev_opps,
        LAG(closed_won, 1) OVER (ORDER BY month) AS prev_won
    FROM monthly
)
SELECT
    month,
    new_leads,
    mqls,
    sqls,
    opps,
    closed_won,
    ROUND(100.0 * (mqls - prev_mqls) / NULLIF(prev_mqls, 0), 1) AS mql_mom_pct,
    ROUND(100.0 * (sqls - prev_sqls) / NULLIF(prev_sqls, 0), 1) AS sql_mom_pct,
    ROUND(100.0 * (opps - prev_opps) / NULLIF(prev_opps, 0), 1) AS opp_mom_pct,
    ROUND(100.0 * (closed_won - prev_won) / NULLIF(prev_won, 0), 1) AS won_mom_pct
FROM with_lag
ORDER BY month;

-- --------------------------------------------------------------------------
-- Query 4: Conversion Rate by Lead Source (with revenue ranking)
-- --------------------------------------------------------------------------
WITH source_metrics AS (
    SELECT
        c.lead_source,
        COUNT(DISTINCT c.contact_id)                               AS total_contacts,
        COUNT(DISTINCT CASE WHEN c.mql_date IS NOT NULL
                            THEN c.contact_id END)                  AS mql_contacts,
        COUNT(DISTINCT CASE WHEN c.opportunity_date IS NOT NULL
                            THEN c.contact_id END)                  AS opp_contacts,
        COUNT(DISTINCT CASE WHEN c.lead_status = 'Closed_Won'
                            THEN c.contact_id END)                  AS won_contacts,
        SUM(CASE WHEN o.closed_won = 1 THEN o.amount ELSE 0 END)   AS attributed_revenue,
        AVG(CASE WHEN o.closed_won = 1 THEN o.amount END)          AS avg_deal_size
    FROM contacts c
    LEFT JOIN opportunities o ON o.primary_contact_id = c.contact_id
    GROUP BY c.lead_source
)
SELECT
    lead_source,
    total_contacts,
    mql_contacts,
    opp_contacts,
    won_contacts,
    ROUND(1.0 * mql_contacts  / NULLIF(total_contacts, 0), 4) AS mql_rate,
    ROUND(1.0 * opp_contacts  / NULLIF(mql_contacts,   0), 4) AS mql_to_opp_rate,
    ROUND(1.0 * won_contacts  / NULLIF(opp_contacts,   0), 4) AS win_rate,
    ROUND(attributed_revenue, 0)                               AS attributed_revenue,
    ROUND(avg_deal_size, 0)                                    AS avg_deal_size,
    RANK() OVER (ORDER BY attributed_revenue DESC)             AS revenue_rank
FROM source_metrics
ORDER BY attributed_revenue DESC;

-- --------------------------------------------------------------------------
-- Query 5: Disqualification and Recycling Analysis
-- --------------------------------------------------------------------------
WITH exit_summary AS (
    SELECT
        stage_name,
        exit_reason,
        COUNT(*)  AS count,
        COUNT(*) OVER (PARTITION BY stage_name) AS stage_total
    FROM lead_stages
    WHERE exit_reason IS NOT NULL
    GROUP BY stage_name, exit_reason
),
disq_by_stage AS (
    SELECT
        stage_name,
        SUM(CASE WHEN exit_reason = 'Disqualified' THEN count ELSE 0 END) AS disqualified,
        SUM(CASE WHEN exit_reason = 'Recycled'     THEN count ELSE 0 END) AS recycled,
        MAX(stage_total)                                                   AS total_exits,
        ROUND(1.0 * SUM(CASE WHEN exit_reason = 'Disqualified' THEN count ELSE 0 END)
              / NULLIF(MAX(stage_total), 0), 4)                            AS disq_rate,
        ROUND(1.0 * SUM(CASE WHEN exit_reason = 'Recycled'     THEN count ELSE 0 END)
              / NULLIF(MAX(stage_total), 0), 4)                            AS recycle_rate
    FROM exit_summary
    GROUP BY stage_name
)
SELECT
    stage_name,
    total_exits,
    disqualified,
    recycled,
    ROUND(disq_rate * 100, 2)    AS disq_pct,
    ROUND(recycle_rate * 100, 2) AS recycle_pct
FROM disq_by_stage
ORDER BY CASE stage_name
    WHEN 'New_Lead'    THEN 1 WHEN 'MQL'  THEN 2 WHEN 'SQL'  THEN 3
    WHEN 'SAL'         THEN 4 WHEN 'Opportunity' THEN 5 ELSE 6
END;
"""

# ---------------------------------------------------------------------------
# Python wrappers
# ---------------------------------------------------------------------------

def get_connection():
    return sqlite3.connect(config.DB_PATH)

def run_funnel_waterfall(conn) -> pd.DataFrame:
    sql = """
    WITH stage_counts AS (
        SELECT
            COUNT(*) AS total_leads,
            COUNT(CASE WHEN mql_date IS NOT NULL THEN 1 END) AS total_mql,
            COUNT(CASE WHEN sql_date IS NOT NULL THEN 1 END) AS total_sql,
            COUNT(CASE WHEN sal_date IS NOT NULL THEN 1 END) AS total_sal,
            COUNT(CASE WHEN opportunity_date IS NOT NULL THEN 1 END) AS total_opp,
            COUNT(CASE WHEN lead_status = 'Closed_Won' THEN 1 END) AS total_won
        FROM contacts
    )
    SELECT
        total_leads, total_mql, total_sql, total_sal, total_opp, total_won,
        ROUND(1.0*total_mql/NULLIF(total_leads,0),4) AS lead_to_mql,
        ROUND(1.0*total_sql/NULLIF(total_mql,  0),4) AS mql_to_sql,
        ROUND(1.0*total_sal/NULLIF(total_sql,  0),4) AS sql_to_sal,
        ROUND(1.0*total_opp/NULLIF(total_sal,  0),4) AS sal_to_opp,
        ROUND(1.0*total_won/NULLIF(total_opp,  0),4) AS opp_to_won,
        ROUND(1.0*total_won/NULLIF(total_leads,0),4) AS cumulative_top_to_won
    FROM stage_counts
    """
    return pd.read_sql_query(sql, conn)

def run_stage_durations(conn) -> pd.DataFrame:
    sql = """
    WITH ranked AS (
        SELECT stage_name, days_in_stage,
               NTILE(2)  OVER (PARTITION BY stage_name ORDER BY days_in_stage) AS half,
               NTILE(4)  OVER (PARTITION BY stage_name ORDER BY days_in_stage) AS quartile,
               NTILE(10) OVER (PARTITION BY stage_name ORDER BY days_in_stage) AS decile
        FROM lead_stages
        WHERE days_in_stage >= 0 AND days_in_stage IS NOT NULL
          AND stage_name IN ('MQL','SQL','SAL','Opportunity','Negotiation')
    )
    SELECT
        stage_name,
        COUNT(*) AS n,
        ROUND(AVG(days_in_stage),1)                          AS avg_days,
        MIN(CASE WHEN half=1    THEN days_in_stage END)      AS median_approx,
        MIN(CASE WHEN quartile=3 THEN days_in_stage END)     AS p75_days,
        MIN(CASE WHEN decile=9   THEN days_in_stage END)     AS p90_days
    FROM ranked
    GROUP BY stage_name
    ORDER BY CASE stage_name
        WHEN 'MQL' THEN 1 WHEN 'SQL' THEN 2 WHEN 'SAL' THEN 3
        WHEN 'Opportunity' THEN 4 WHEN 'Negotiation' THEN 5 END
    """
    return pd.read_sql_query(sql, conn)

def run_monthly_velocity(conn) -> pd.DataFrame:
    sql = """
    WITH monthly AS (
        SELECT SUBSTR(created_date,1,7) AS month,
               COUNT(*) AS new_leads,
               COUNT(CASE WHEN mql_date IS NOT NULL THEN 1 END) AS mqls,
               COUNT(CASE WHEN sql_date IS NOT NULL THEN 1 END) AS sqls,
               COUNT(CASE WHEN opportunity_date IS NOT NULL THEN 1 END) AS opps,
               COUNT(CASE WHEN lead_status='Closed_Won' THEN 1 END) AS closed_won
        FROM contacts GROUP BY SUBSTR(created_date,1,7)
    )
    SELECT month, new_leads, mqls, sqls, opps, closed_won,
           LAG(mqls,1) OVER (ORDER BY month) AS prev_mqls,
           ROUND(100.0*(mqls - LAG(mqls,1) OVER (ORDER BY month))
                 / NULLIF(LAG(mqls,1) OVER (ORDER BY month),0),1) AS mql_mom_pct,
           ROUND(100.0*(opps - LAG(opps,1) OVER (ORDER BY month))
                 / NULLIF(LAG(opps,1) OVER (ORDER BY month),0),1) AS opp_mom_pct
    FROM monthly ORDER BY month
    """
    return pd.read_sql_query(sql, conn)

def run_conversion_by_source(conn) -> pd.DataFrame:
    sql = """
    WITH sm AS (
        SELECT c.lead_source,
               COUNT(DISTINCT c.contact_id)                                     AS total_contacts,
               COUNT(DISTINCT CASE WHEN c.mql_date IS NOT NULL THEN c.contact_id END) AS mql_contacts,
               COUNT(DISTINCT CASE WHEN c.opportunity_date IS NOT NULL THEN c.contact_id END) AS opp_contacts,
               COUNT(DISTINCT CASE WHEN c.lead_status='Closed_Won' THEN c.contact_id END) AS won_contacts,
               SUM(CASE WHEN o.closed_won=1 THEN o.amount ELSE 0 END) AS attributed_revenue,
               AVG(CASE WHEN o.closed_won=1 THEN o.amount END)        AS avg_deal_size
        FROM contacts c
        LEFT JOIN opportunities o ON o.primary_contact_id=c.contact_id
        GROUP BY c.lead_source
    )
    SELECT lead_source, total_contacts, mql_contacts, opp_contacts, won_contacts,
           ROUND(1.0*mql_contacts/NULLIF(total_contacts,0),4) AS mql_rate,
           ROUND(1.0*won_contacts/NULLIF(opp_contacts,  0),4) AS win_rate,
           ROUND(attributed_revenue,0) AS attributed_revenue,
           ROUND(avg_deal_size,0)      AS avg_deal_size,
           RANK() OVER (ORDER BY attributed_revenue DESC) AS revenue_rank
    FROM sm ORDER BY attributed_revenue DESC
    """
    return pd.read_sql_query(sql, conn)

def run_all(conn=None) -> dict:
    if conn is None:
        conn = get_connection()
    return {
        "funnel_waterfall":   run_funnel_waterfall(conn),
        "stage_durations":    run_stage_durations(conn),
        "monthly_velocity":   run_monthly_velocity(conn),
        "conversion_by_source": run_conversion_by_source(conn),
    }

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Running funnel metrics...")
    conn    = get_connection()
    results = run_all(conn)

    print("\n--- Funnel Waterfall ---")
    print(results["funnel_waterfall"].to_string(index=False))

    print("\n--- Stage Durations ---")
    print(results["stage_durations"].to_string(index=False))

    print("\n--- Conversion by Source (top 5) ---")
    print(results["conversion_by_source"].head().to_string(index=False))

    # Write SQL to file
    os.makedirs("sql", exist_ok=True)
    with open("sql/01_funnel_metrics.sql", "w") as f:
        f.write(FUNNEL_METRICS_SQL)
    print("\n✓ sql/01_funnel_metrics.sql written")
    conn.close()

if __name__ == "__main__":
    main()
