
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
