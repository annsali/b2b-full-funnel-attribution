-- ==========================================================================
-- 06_executive_summary.sql
-- High-level KPIs for the executive dashboard.
-- These queries are designed to be refreshed daily and consumed by Tableau.
-- ==========================================================================

-- --------------------------------------------------------------------------
-- Query 1: Top-Level KPI Summary
-- --------------------------------------------------------------------------
WITH funnel AS (
    SELECT
        COUNT(*)                                                        AS total_leads,
        COUNT(CASE WHEN mql_date IS NOT NULL THEN 1 END)               AS total_mqls,
        COUNT(CASE WHEN sql_date IS NOT NULL THEN 1 END)               AS total_sqls,
        COUNT(CASE WHEN sal_date IS NOT NULL THEN 1 END)               AS total_sals,
        COUNT(CASE WHEN opportunity_date IS NOT NULL THEN 1 END)       AS total_opps,
        COUNT(CASE WHEN lead_status = 'Closed_Won' THEN 1 END)        AS total_won
    FROM contacts
),
revenue AS (
    SELECT
        ROUND(SUM(CASE WHEN closed_won = 1 THEN amount ELSE 0 END), 0) AS total_revenue,
        ROUND(AVG(CASE WHEN closed_won = 1 THEN amount END), 0)        AS avg_deal_size,
        COUNT(CASE WHEN closed_won = 1 THEN 1 END)                     AS won_deals,
        COUNT(CASE WHEN closed_won = 0 AND close_date IS NOT NULL THEN 1 END) AS lost_deals
    FROM opportunities
)
SELECT
    f.total_leads,
    f.total_mqls,
    f.total_sqls,
    f.total_sals,
    f.total_opps,
    f.total_won,
    r.total_revenue,
    r.avg_deal_size,
    r.won_deals,
    r.lost_deals,
    -- Conversion rates
    ROUND(1.0 * f.total_mqls / NULLIF(f.total_leads, 0), 4)           AS lead_to_mql_rate,
    ROUND(1.0 * f.total_sqls / NULLIF(f.total_mqls,  0), 4)           AS mql_to_sql_rate,
    ROUND(1.0 * f.total_sals / NULLIF(f.total_sqls,  0), 4)           AS sql_to_sal_rate,
    ROUND(1.0 * f.total_opps / NULLIF(f.total_sals,  0), 4)           AS sal_to_opp_rate,
    ROUND(1.0 * f.total_won  / NULLIF(f.total_opps,  0), 4)           AS win_rate,
    ROUND(1.0 * f.total_won  / NULLIF(f.total_leads, 0), 4)           AS end_to_end_rate,
    -- Pipeline metrics
    ROUND(r.total_revenue / NULLIF(f.total_mqls, 0), 0)               AS revenue_per_mql,
    ROUND(r.total_revenue / NULLIF(f.total_leads, 0), 0)              AS revenue_per_lead
FROM funnel f, revenue r;

-- --------------------------------------------------------------------------
-- Query 2: Revenue by Quarter
-- --------------------------------------------------------------------------
WITH quarterly AS (
    SELECT
        CASE
            WHEN CAST(SUBSTR(c.closed_date, 6, 2) AS INT) BETWEEN 1 AND 3  THEN
                SUBSTR(c.closed_date, 1, 4) || '-Q1'
            WHEN CAST(SUBSTR(c.closed_date, 6, 2) AS INT) BETWEEN 4 AND 6  THEN
                SUBSTR(c.closed_date, 1, 4) || '-Q2'
            WHEN CAST(SUBSTR(c.closed_date, 6, 2) AS INT) BETWEEN 7 AND 9  THEN
                SUBSTR(c.closed_date, 1, 4) || '-Q3'
            ELSE
                SUBSTR(c.closed_date, 1, 4) || '-Q4'
        END                                                             AS quarter,
        COUNT(DISTINCT c.contact_id)                                    AS won_deals,
        SUM(o.amount)                                                   AS revenue,
        AVG(o.amount)                                                   AS avg_deal_size
    FROM contacts c
    JOIN opportunities o ON o.primary_contact_id = c.contact_id
    WHERE c.lead_status = 'Closed_Won'
      AND c.closed_date IS NOT NULL
    GROUP BY quarter
)
SELECT
    quarter,
    won_deals,
    ROUND(revenue, 0)                                                   AS revenue,
    ROUND(avg_deal_size, 0)                                             AS avg_deal_size,
    SUM(revenue) OVER (ORDER BY quarter ROWS UNBOUNDED PRECEDING)      AS cumulative_revenue,
    LAG(revenue, 1) OVER (ORDER BY quarter)                            AS prior_quarter_revenue,
    ROUND(100.0 * (revenue - LAG(revenue,1) OVER (ORDER BY quarter))
          / NULLIF(LAG(revenue,1) OVER (ORDER BY quarter), 0), 1)     AS qoq_growth_pct
FROM quarterly
ORDER BY quarter;

-- --------------------------------------------------------------------------
-- Query 3: Top 10 Industries by Revenue
-- --------------------------------------------------------------------------
SELECT
    a.industry,
    COUNT(DISTINCT c.contact_id)                                       AS total_contacts,
    COUNT(DISTINCT CASE WHEN c.lead_status = 'Closed_Won'
                        THEN c.contact_id END)                         AS won_contacts,
    ROUND(SUM(CASE WHEN o.closed_won = 1 THEN o.amount ELSE 0 END), 0) AS total_revenue,
    ROUND(AVG(CASE WHEN o.closed_won = 1 THEN o.amount END), 0)       AS avg_deal_size,
    ROUND(1.0 * COUNT(DISTINCT CASE WHEN c.lead_status = 'Closed_Won'
                                    THEN c.contact_id END)
          / NULLIF(COUNT(DISTINCT CASE WHEN c.opportunity_date IS NOT NULL
                                       THEN c.contact_id END), 0), 4) AS win_rate,
    RANK() OVER (ORDER BY SUM(CASE WHEN o.closed_won=1 THEN o.amount ELSE 0 END) DESC) AS rank
FROM contacts c
JOIN accounts a ON a.account_id = c.account_id
LEFT JOIN opportunities o ON o.primary_contact_id = c.contact_id
GROUP BY a.industry
ORDER BY total_revenue DESC
LIMIT 10;

-- --------------------------------------------------------------------------
-- Query 4: Product Line Performance
-- --------------------------------------------------------------------------
SELECT
    o.product_line,
    COUNT(o.opportunity_id)                                            AS total_opps,
    COUNT(CASE WHEN o.closed_won = 1 THEN 1 END)                      AS won_opps,
    ROUND(AVG(CASE WHEN o.closed_won=1 THEN 1.0 ELSE 0.0 END), 3)    AS win_rate,
    ROUND(SUM(CASE WHEN o.closed_won=1 THEN o.amount ELSE 0 END), 0) AS total_revenue,
    ROUND(AVG(CASE WHEN o.closed_won=1 THEN o.amount END), 0)        AS avg_deal_size,
    ROUND(AVG(o.days_to_close), 1)                                    AS avg_days_to_close
FROM opportunities o
GROUP BY o.product_line
ORDER BY total_revenue DESC;

-- --------------------------------------------------------------------------
-- Query 5: Open Pipeline Forecast
-- Estimate future revenue from open opportunities using historical win rates.
-- --------------------------------------------------------------------------
WITH open_pipeline AS (
    SELECT
        o.stage,
        COUNT(o.opportunity_id)                                        AS open_opps,
        SUM(o.amount)                                                  AS total_pipeline,
        AVG(o.win_probability)                                         AS avg_win_prob
    FROM opportunities o
    WHERE o.stage NOT IN ('Closed_Won', 'Closed_Lost')
    GROUP BY o.stage
),
stage_win_rate AS (
    SELECT
        stage,
        ROUND(AVG(CASE WHEN closed_won=1 THEN 1.0 ELSE 0.0 END), 3) AS historical_win_rate
    FROM opportunities
    WHERE stage IN ('Closed_Won', 'Closed_Lost')
    GROUP BY stage
)
SELECT
    p.stage,
    p.open_opps,
    ROUND(p.total_pipeline, 0)                                         AS total_pipeline,
    ROUND(p.avg_win_prob, 3)                                           AS avg_win_probability,
    ROUND(p.total_pipeline * p.avg_win_prob, 0)                        AS forecasted_revenue
FROM open_pipeline p
ORDER BY forecasted_revenue DESC;
