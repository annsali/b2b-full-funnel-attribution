
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
