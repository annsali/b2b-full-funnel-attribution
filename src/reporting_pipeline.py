"""
Automated Reporting Pipeline — daily, weekly, and monthly marketing reports.

Run: python -m src.reporting_pipeline
"""
import os, sys, sqlite3
from datetime import datetime
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

AUTOMATED_REPORTS_SQL = """
-- ==========================================================================
-- 07_automated_reports.sql
-- Scheduled report queries: daily, weekly, monthly.
-- ==========================================================================

-- DAILY REPORT -------------------------------------------------------
-- New MQLs today vs 7-day average
WITH daily_mql AS (
    SELECT COUNT(*) AS today_mqls
    FROM contacts
    WHERE mql_date = DATE('now')
),
avg_7d AS (
    SELECT ROUND(COUNT(*) / 7.0, 1) AS avg_7d_mqls
    FROM contacts
    WHERE mql_date >= DATE('now', '-7 days')
      AND mql_date <  DATE('now')
)
SELECT
    dm.today_mqls,
    a7.avg_7d_mqls,
    ROUND(dm.today_mqls - a7.avg_7d_mqls, 1) AS delta_vs_avg
FROM daily_mql dm, avg_7d a7;

-- Funnel snapshot: count at each stage right now
SELECT
    lead_status AS current_stage,
    COUNT(*) AS contacts_in_stage,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_of_total
FROM contacts
GROUP BY lead_status
ORDER BY CASE lead_status
    WHEN 'New_Lead'    THEN 1 WHEN 'MQL'  THEN 2 WHEN 'SQL'  THEN 3
    WHEN 'SAL'         THEN 4 WHEN 'Opportunity' THEN 5 WHEN 'Negotiation' THEN 6
    WHEN 'Closed_Won'  THEN 7 WHEN 'Closed_Lost' THEN 8 ELSE 9
END;

-- WEEKLY REPORT -------------------------------------------------------
-- Funnel conversion rates: this week vs last week vs 4-week avg
WITH weekly AS (
    SELECT
        CASE
            WHEN created_date >= DATE('now', '-7 days')  THEN 'this_week'
            WHEN created_date >= DATE('now', '-14 days') THEN 'last_week'
            WHEN created_date >= DATE('now', '-28 days') THEN 'prior_4wk'
            ELSE NULL
        END AS period,
        COUNT(*) AS leads,
        COUNT(CASE WHEN mql_date IS NOT NULL THEN 1 END) AS mqls,
        COUNT(CASE WHEN opportunity_date IS NOT NULL THEN 1 END) AS opps,
        COUNT(CASE WHEN lead_status='Closed_Won' THEN 1 END) AS won
    FROM contacts
    WHERE created_date >= DATE('now', '-28 days')
    GROUP BY period
)
SELECT period, leads, mqls, opps, won,
       ROUND(1.0*mqls/NULLIF(leads,0),3) AS mql_rate,
       ROUND(1.0*won /NULLIF(opps, 0),3) AS win_rate
FROM weekly
WHERE period IS NOT NULL
ORDER BY CASE period WHEN 'this_week' THEN 1 WHEN 'last_week' THEN 2 ELSE 3 END;

-- MONTHLY REPORT -------------------------------------------------------
-- Executive summary: key KPIs for current month vs prior month
WITH cur AS (
    SELECT 'current_month' AS period,
           COUNT(DISTINCT CASE WHEN SUBSTR(mql_date,1,7)=STRFTIME('%Y-%m',DATE('now'))
                               THEN contact_id END) AS mqls,
           COUNT(DISTINCT CASE WHEN SUBSTR(opportunity_date,1,7)=STRFTIME('%Y-%m',DATE('now'))
                               THEN contact_id END) AS opps,
           COUNT(DISTINCT CASE WHEN lead_status='Closed_Won'
                               AND SUBSTR(closed_date,1,7)=STRFTIME('%Y-%m',DATE('now'))
                               THEN contact_id END) AS won
    FROM contacts
),
prior AS (
    SELECT 'prior_month' AS period,
           COUNT(DISTINCT CASE WHEN SUBSTR(mql_date,1,7)=STRFTIME('%Y-%m',DATE('now','-1 month'))
                               THEN contact_id END) AS mqls,
           COUNT(DISTINCT CASE WHEN SUBSTR(opportunity_date,1,7)=STRFTIME('%Y-%m',DATE('now','-1 month'))
                               THEN contact_id END) AS opps,
           COUNT(DISTINCT CASE WHEN lead_status='Closed_Won'
                               AND SUBSTR(closed_date,1,7)=STRFTIME('%Y-%m',DATE('now','-1 month'))
                               THEN contact_id END) AS won
    FROM contacts
)
SELECT * FROM cur
UNION ALL
SELECT * FROM prior;
"""

# ---------------------------------------------------------------------------
# Report generators
# ---------------------------------------------------------------------------

def daily_report(conn) -> dict:
    snapshot_sql = """
    SELECT lead_status AS stage, COUNT(*) AS count
    FROM contacts GROUP BY lead_status
    ORDER BY CASE lead_status
        WHEN 'New_Lead' THEN 1 WHEN 'MQL' THEN 2 WHEN 'SQL' THEN 3
        WHEN 'SAL' THEN 4 WHEN 'Opportunity' THEN 5 WHEN 'Negotiation' THEN 6
        WHEN 'Closed_Won' THEN 7 WHEN 'Closed_Lost' THEN 8 ELSE 9 END
    """
    recent_mql_sql = """
    SELECT COUNT(*) AS recent_mqls,
           ROUND(COUNT(*) / 7.0, 1) AS daily_avg
    FROM contacts
    WHERE mql_date >= (SELECT DATE(MAX(mql_date), '-7 days') FROM contacts)
    """

    snapshot = pd.read_sql_query(snapshot_sql, conn)
    mql_info = pd.read_sql_query(recent_mql_sql, conn)

    total_won_sql = """
    SELECT ROUND(SUM(o.amount),0) AS total_closed_won_revenue
    FROM opportunities o
    WHERE o.closed_won = 1
    """
    revenue = pd.read_sql_query(total_won_sql, conn)

    return {
        "report_type":   "daily",
        "last_refreshed": datetime.now().isoformat(),
        "funnel_snapshot": snapshot,
        "mql_7d_avg":    mql_info,
        "total_revenue": revenue,
    }


def weekly_report(conn) -> dict:
    velocity_sql = """
    WITH monthly AS (
        SELECT SUBSTR(created_date,1,7) AS month,
               COUNT(CASE WHEN mql_date IS NOT NULL THEN 1 END) AS mqls,
               COUNT(CASE WHEN opportunity_date IS NOT NULL THEN 1 END) AS opps
        FROM contacts
        GROUP BY SUBSTR(created_date,1,7)
    )
    SELECT month, mqls, opps,
           ROUND(100.0*(mqls - LAG(mqls,1) OVER (ORDER BY month))
                 / NULLIF(LAG(mqls,1) OVER (ORDER BY month),0),1) AS mql_lvr_pct
    FROM monthly
    ORDER BY month DESC LIMIT 8
    """
    campaign_sql = """
    WITH co AS (
        SELECT c.contact_id, o.amount
        FROM contacts c
        JOIN opportunities o ON o.primary_contact_id=c.contact_id
        WHERE o.closed_won=1
    ),
    td AS (
        SELECT t.campaign_name, t.contact_id, co.amount,
               1.0/COUNT(*) OVER (PARTITION BY t.contact_id) AS w
        FROM touchpoints t JOIN co ON co.contact_id=t.contact_id
    )
    SELECT campaign_name, ROUND(SUM(w*amount),0) AS attributed_revenue,
           COUNT(DISTINCT contact_id) AS contacts
    FROM td GROUP BY campaign_name
    ORDER BY attributed_revenue DESC LIMIT 10
    """
    bottleneck_sql = """
    SELECT stage_name,
           ROUND(AVG(days_in_stage),1) AS avg_days,
           COUNT(*) AS volume
    FROM lead_stages
    WHERE days_in_stage >= 0
    GROUP BY stage_name
    ORDER BY avg_days DESC
    """

    return {
        "report_type":   "weekly",
        "last_refreshed": datetime.now().isoformat(),
        "velocity_trend": pd.read_sql_query(velocity_sql, conn),
        "top_campaigns":  pd.read_sql_query(campaign_sql, conn),
        "bottlenecks":    pd.read_sql_query(bottleneck_sql, conn),
    }


def monthly_report(conn) -> dict:
    exec_sql = """
    SELECT
        COUNT(*) AS total_leads,
        COUNT(CASE WHEN mql_date IS NOT NULL THEN 1 END) AS total_mqls,
        COUNT(CASE WHEN opportunity_date IS NOT NULL THEN 1 END) AS total_opps,
        COUNT(CASE WHEN lead_status='Closed_Won' THEN 1 END) AS total_won,
        ROUND(1.0*COUNT(CASE WHEN lead_status='Closed_Won' THEN 1 END)
              / NULLIF(COUNT(CASE WHEN opportunity_date IS NOT NULL THEN 1 END),0),3) AS win_rate
    FROM contacts
    """
    revenue_sql = """
    SELECT ROUND(SUM(CASE WHEN closed_won=1 THEN amount ELSE 0 END),0) AS total_revenue,
           ROUND(AVG(CASE WHEN closed_won=1 THEN amount END),0) AS avg_deal_size,
           COUNT(CASE WHEN closed_won=1 THEN 1 END) AS won_deals
    FROM opportunities
    """
    channel_sql = """
    WITH ft AS (
        SELECT contact_id, channel
        FROM (SELECT contact_id, channel,
                     ROW_NUMBER() OVER (PARTITION BY contact_id ORDER BY touchpoint_timestamp) rn
              FROM touchpoints) WHERE rn=1
    )
    SELECT ft.channel,
           COUNT(DISTINCT c.contact_id) AS leads,
           COUNT(DISTINCT CASE WHEN c.lead_status='Closed_Won' THEN c.contact_id END) AS won,
           SUM(CASE WHEN o.closed_won=1 THEN o.amount ELSE 0 END) AS revenue
    FROM contacts c
    LEFT JOIN ft ON ft.contact_id=c.contact_id
    LEFT JOIN opportunities o ON o.primary_contact_id=c.contact_id
    GROUP BY ft.channel ORDER BY revenue DESC
    """

    exec_summary = pd.read_sql_query(exec_sql, conn)
    revenue      = pd.read_sql_query(revenue_sql, conn)
    by_channel   = pd.read_sql_query(channel_sql, conn)

    return {
        "report_type":    "monthly",
        "last_refreshed": datetime.now().isoformat(),
        "exec_summary":   exec_summary,
        "revenue":        revenue,
        "by_channel":     by_channel,
    }


def print_report(report: dict):
    print(f"\n{'='*60}")
    print(f"  {report['report_type'].upper()} REPORT — {report['last_refreshed']}")
    print(f"{'='*60}")
    for key, val in report.items():
        if key in ("report_type", "last_refreshed"):
            continue
        print(f"\n  {key.upper().replace('_',' ')}:")
        if isinstance(val, pd.DataFrame):
            print(val.to_string(index=False))
        else:
            print(f"  {val}")


def export_reports(reports: list, output_dir: str = "data"):
    os.makedirs(output_dir, exist_ok=True)
    for r in reports:
        rtype = r["report_type"]
        for key, val in r.items():
            if isinstance(val, pd.DataFrame):
                path = os.path.join(output_dir, f"report_{rtype}_{key}.csv")
                val.to_csv(path, index=False)


def main():
    conn = sqlite3.connect(config.DB_PATH)

    daily   = daily_report(conn)
    weekly  = weekly_report(conn)
    monthly = monthly_report(conn)

    for r in [daily, weekly, monthly]:
        print_report(r)

    export_reports([daily, weekly, monthly])
    print("\n✓ Reports exported to data/")

    os.makedirs("sql", exist_ok=True)
    with open("sql/07_automated_reports.sql", "w") as f:
        f.write(AUTOMATED_REPORTS_SQL)
    print("✓ sql/07_automated_reports.sql written")
    conn.close()


if __name__ == "__main__":
    main()
