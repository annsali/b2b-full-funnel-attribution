
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
