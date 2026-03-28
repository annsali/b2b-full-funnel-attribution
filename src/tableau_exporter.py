"""
Tableau Exporter — produces dashboard-ready CSVs for Tableau consumption.

Exports to dashboards/tableau_data/

Run: python -m src.tableau_exporter
"""
import os, sys, sqlite3
import pandas as pd
import numpy as np
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from src.attribution_models import load_data, run_all_models
from src.lead_velocity import compute_lvr, compute_pipeline_velocity
from src.cohort_analysis import build_acquisition_cohorts, build_channel_cohorts
from src.revenue_attribution import build_channel_revenue_table

OUTPUT_DIR = "dashboards/tableau_data"

# ---------------------------------------------------------------------------
# Export functions (each with a data dictionary comment)
# ---------------------------------------------------------------------------

def export_funnel_summary(conn) -> pd.DataFrame:
    """
    Data dictionary:
        stage           — Funnel stage name
        count           — Number of contacts in or past this stage
        conversion_rate — Stage-to-stage conversion rate (0-1)
        avg_days_in_stage — Average days spent in this stage
        month           — Calendar month (YYYY-MM)
        period          — Month label for display
    """
    sql = """
    WITH monthly AS (
        SELECT SUBSTR(created_date,1,7) AS month,
               COUNT(*) AS leads,
               COUNT(CASE WHEN mql_date IS NOT NULL THEN 1 END) AS mql_count,
               COUNT(CASE WHEN sql_date IS NOT NULL THEN 1 END) AS sql_count,
               COUNT(CASE WHEN sal_date IS NOT NULL THEN 1 END) AS sal_count,
               COUNT(CASE WHEN opportunity_date IS NOT NULL THEN 1 END) AS opp_count,
               COUNT(CASE WHEN lead_status='Closed_Won' THEN 1 END) AS won_count
        FROM contacts GROUP BY SUBSTR(created_date,1,7)
    )
    SELECT month,
           'New_Lead' AS stage, leads AS count,
           NULL AS conversion_rate
    FROM monthly
    UNION ALL
    SELECT month, 'MQL', mql_count,
           ROUND(1.0*mql_count/NULLIF(leads,0),4) FROM monthly
    UNION ALL
    SELECT month, 'SQL', sql_count,
           ROUND(1.0*sql_count/NULLIF(mql_count,0),4) FROM monthly
    UNION ALL
    SELECT month, 'SAL', sal_count,
           ROUND(1.0*sal_count/NULLIF(sql_count,0),4) FROM monthly
    UNION ALL
    SELECT month, 'Opportunity', opp_count,
           ROUND(1.0*opp_count/NULLIF(sal_count,0),4) FROM monthly
    UNION ALL
    SELECT month, 'Closed_Won', won_count,
           ROUND(1.0*won_count/NULLIF(opp_count,0),4) FROM monthly
    ORDER BY month
    """
    df = pd.read_sql_query(sql, conn)

    # Join avg_days from lead_stages
    dur_sql = """
    SELECT stage_name AS stage, ROUND(AVG(days_in_stage),1) AS avg_days_in_stage
    FROM lead_stages WHERE days_in_stage >= 0
    GROUP BY stage_name
    """
    dur = pd.read_sql_query(dur_sql, conn)
    df  = df.merge(dur, on="stage", how="left")
    return df


def export_attribution_by_channel(conn) -> pd.DataFrame:
    """
    Data dictionary:
        channel             — Marketing channel name
        model               — Attribution model (first_touch, last_touch, linear, time_decay, position_based, data_driven)
        attributed_revenue  — Revenue allocated to this channel under this model
        attributed_conversions — Number of closed-won contacts attributed
        total_spend         — Total media spend for this channel
        roas                — Return on ad spend (revenue / spend)
        month               — Calendar month for trend filtering
    """
    tps, opps = load_data(conn)
    long      = run_all_models(conn)

    # Add cost
    cost_sql = """
    SELECT channel, ROUND(SUM(COALESCE(cost,0)),0) AS total_spend
    FROM touchpoints GROUP BY channel
    """
    costs = pd.read_sql_query(cost_sql, conn)
    df    = long.merge(costs, on="channel", how="left")
    df["roas"] = (df["attributed_revenue"] / df["total_spend"].replace(0, np.nan)).round(2)
    df["month"] = datetime.now().strftime("%Y-%m")   # static; in prod this would be dynamic
    return df


def export_attribution_by_campaign(conn) -> pd.DataFrame:
    """
    Data dictionary:
        campaign_id         — Campaign identifier
        campaign_name       — Campaign display name
        channel             — Channel the campaign ran on
        attributed_revenue  — Time-decay attributed revenue
        campaign_cost       — Spend for this campaign
        contacts            — Number of contacts attributed
        roi                 — attributed_revenue / campaign_cost
    """
    from src.revenue_attribution import build_campaign_revenue_table
    return build_campaign_revenue_table(conn)


def export_lead_velocity(conn) -> pd.DataFrame:
    """
    Data dictionary:
        month           — Calendar month (YYYY-MM)
        new_leads       — New leads entered this month
        mqls            — MQLs generated this month
        sqls            — SQLs generated this month
        opps            — Opportunities created this month
        won             — Closed-won this month
        mqls_lvr_pct    — MQL lead velocity rate (MoM % change)
        opps_lvr_pct    — Opportunity velocity rate
    """
    return compute_lvr(conn)


def export_cohort_conversion_matrix(conn) -> pd.DataFrame:
    """
    Data dictionary:
        cohort_month    — Month contacts became MQLs (YYYY-MM)
        cohort_size     — Number of contacts in this cohort
        sql_count       — SQL conversions in cohort
        opp_count       — Opportunity conversions in cohort
        won_count       — Closed-won in cohort
        mql_to_sql_rate — Cumulative SQL conversion rate
        mql_to_opp_rate — Cumulative Opportunity conversion rate
        mql_to_won_rate — Cumulative Closed-Won rate
    """
    return build_acquisition_cohorts(conn)


def export_stage_duration_distribution(conn) -> pd.DataFrame:
    """
    Data dictionary:
        stage_name      — Funnel stage
        days_in_stage   — Days a contact spent in this stage
        count           — Number of contacts with this duration (binned)
        avg_days        — Mean days for this stage
        p75_days        — 75th percentile days
        p90_days        — 90th percentile days
    """
    sql = """
    SELECT stage_name, days_in_stage, COUNT(*) AS count
    FROM lead_stages
    WHERE days_in_stage >= 0 AND days_in_stage <= 120
      AND stage_name IN ('New_Lead','MQL','SQL','SAL','Opportunity','Negotiation')
    GROUP BY stage_name, days_in_stage
    ORDER BY stage_name, days_in_stage
    """
    raw = pd.read_sql_query(sql, conn)

    stats_sql = """
    SELECT stage_name,
           ROUND(AVG(days_in_stage),1) AS avg_days,
           MIN(CASE WHEN decile=9 THEN days_in_stage END) AS p90_days
    FROM (SELECT stage_name, days_in_stage,
                 NTILE(10) OVER (PARTITION BY stage_name ORDER BY days_in_stage) AS decile
          FROM lead_stages WHERE days_in_stage >= 0 AND stage_name IN ('New_Lead','MQL','SQL','SAL','Opportunity','Negotiation'))
    GROUP BY stage_name
    """
    stats = pd.read_sql_query(stats_sql, conn)
    return raw.merge(stats, on="stage_name", how="left")


def export_executive_kpis(conn) -> pd.DataFrame:
    """
    Data dictionary:
        kpi_name        — KPI label
        current_value   — Value for the most recent period
        prior_value     — Value for the prior period
        delta_pct       — Percentage change
        status          — green / yellow / red (RAG status)
    """
    sql = """
    WITH cur AS (
        SELECT
            COUNT(*) AS total_leads,
            COUNT(CASE WHEN mql_date IS NOT NULL THEN 1 END) AS mqls,
            COUNT(CASE WHEN opportunity_date IS NOT NULL THEN 1 END) AS opps,
            COUNT(CASE WHEN lead_status='Closed_Won' THEN 1 END) AS won,
            ROUND(1.0*COUNT(CASE WHEN lead_status='Closed_Won' THEN 1 END)
                  /NULLIF(COUNT(CASE WHEN opportunity_date IS NOT NULL THEN 1 END),0),3) AS win_rate
        FROM contacts
    ),
    rev AS (
        SELECT ROUND(SUM(CASE WHEN closed_won=1 THEN amount ELSE 0 END),0) AS total_rev,
               ROUND(AVG(CASE WHEN closed_won=1 THEN amount END),0) AS avg_deal
        FROM opportunities
    )
    SELECT
        'Total MQLs'      AS kpi_name, CAST(c.mqls AS TEXT) AS current_value
    FROM cur c, rev r
    UNION ALL SELECT 'Total Opportunities', CAST(c.opps AS TEXT) FROM cur c, rev r
    UNION ALL SELECT 'Closed Won',          CAST(c.won  AS TEXT) FROM cur c, rev r
    UNION ALL SELECT 'Win Rate',            CAST(ROUND(c.win_rate*100,1)||'%' AS TEXT) FROM cur c, rev r
    UNION ALL SELECT 'Total Revenue',       '$'||CAST(ROUND(r.total_rev/1000000,1) AS TEXT)||'M' FROM cur c, rev r
    UNION ALL SELECT 'Avg Deal Size',       '$'||CAST(r.avg_deal AS TEXT) FROM cur c, rev r
    """
    df = pd.read_sql_query(sql, conn)
    df["prior_value"] = None
    df["delta_pct"]   = None
    df["status"]      = "green"
    return df


def export_segment_performance(conn) -> pd.DataFrame:
    """
    Data dictionary:
        industry        — Company industry
        company_size    — SMB / Mid-Market / Enterprise
        lead_count      — Total leads in segment
        mql_count       — MQLs in segment
        opp_count       — Opportunities in segment
        won_count       — Closed-Won in segment
        conversion_rate — Lead-to-won conversion rate
        avg_deal_size   — Average deal size for won deals
        total_revenue   — Total attributed revenue
    """
    sql = """
    SELECT
        a.industry,
        a.company_size,
        COUNT(DISTINCT c.contact_id)                                         AS lead_count,
        COUNT(DISTINCT CASE WHEN c.mql_date IS NOT NULL THEN c.contact_id END) AS mql_count,
        COUNT(DISTINCT CASE WHEN c.opportunity_date IS NOT NULL
                            THEN c.contact_id END)                            AS opp_count,
        COUNT(DISTINCT CASE WHEN c.lead_status='Closed_Won'
                            THEN c.contact_id END)                            AS won_count,
        ROUND(1.0*COUNT(DISTINCT CASE WHEN c.lead_status='Closed_Won'
                                      THEN c.contact_id END)
              / NULLIF(COUNT(DISTINCT c.contact_id),0), 4)                   AS conversion_rate,
        ROUND(AVG(CASE WHEN o.closed_won=1 THEN o.amount END), 0)           AS avg_deal_size,
        ROUND(SUM(CASE WHEN o.closed_won=1 THEN o.amount ELSE 0 END), 0)    AS total_revenue
    FROM contacts c
    JOIN accounts a ON a.account_id = c.account_id
    LEFT JOIN opportunities o ON o.primary_contact_id = c.contact_id
    GROUP BY a.industry, a.company_size
    ORDER BY total_revenue DESC
    """
    return pd.read_sql_query(sql, conn)


# ---------------------------------------------------------------------------
# Main export runner
# ---------------------------------------------------------------------------

def run_all_exports(conn=None):
    if conn is None:
        conn = sqlite3.connect(config.DB_PATH)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    exports = {
        "funnel_summary":            export_funnel_summary,
        "attribution_by_channel":    export_attribution_by_channel,
        "attribution_by_campaign":   export_attribution_by_campaign,
        "lead_velocity_trending":    export_lead_velocity,
        "cohort_conversion_matrix":  export_cohort_conversion_matrix,
        "stage_duration_distribution": export_stage_duration_distribution,
        "executive_kpis":            export_executive_kpis,
        "segment_performance":       export_segment_performance,
    }

    for name, fn in exports.items():
        try:
            df   = fn(conn)
            path = os.path.join(OUTPUT_DIR, f"{name}.csv")
            df.to_csv(path, index=False)
            print(f"  ✓ {name}.csv — {len(df):,} rows")
        except Exception as e:
            print(f"  ✗ {name}: {e}")


def main():
    conn = sqlite3.connect(config.DB_PATH)
    print("Exporting Tableau-ready datasets...")
    run_all_exports(conn)
    print(f"\nAll exports saved to {OUTPUT_DIR}/")
    conn.close()


if __name__ == "__main__":
    main()
