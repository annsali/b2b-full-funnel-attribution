
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
