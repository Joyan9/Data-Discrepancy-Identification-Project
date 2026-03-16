-- Task 1 - Write a query that shows daily sessions and purchases across the full date range so you can visually identify when things went wrong.

-- I added the revenue field and rolling avg
WITH raw_events_unnested AS (
  SELECT 
    event_date,
    CONCAT(user_pseudo_id, (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) AS session_id,
    ecommerce.transaction_id AS transaction_id,
    ecommerce.purchase_revenue AS purchase_revenue
  FROM `upheld-setting-420306.ga4_dataset.ga4_events` 
  ),

daily_aggregates AS (
SELECT
  event_date,
  COUNT(DISTINCT session_id) AS sessions,
  COUNT(DISTINCT transaction_id) AS purchases,
  SUM(purchase_revenue) AS purchase_revenue,
  ROUND(COUNT(DISTINCT transaction_id) * 100.0 / COUNT(DISTINCT session_id), 2) AS cvr_pct 
FROM raw_events_unnested
GROUP BY event_date
)

SELECT
  *,
  ROUND(AVG(sessions) OVER(ORDER BY event_date ROWS BETWEEN 7 PRECEDING AND CURRENT ROW), 2) AS rolling_7d_sessions,
  ROUND(AVG(purchase_revenue) OVER(ORDER BY event_date ROWS BETWEEN 7 PRECEDING AND CURRENT ROW), 2) AS rolling_7d_purchase_revenue
FROM daily_aggregates
ORDER BY event_date


-- Task 2 - Break down sessions by traffic medium over time. Compare the pre-anomaly period (Jan 1 – Feb 4) vs. the anomaly period (Feb 5 – Mar 1).

WITH daily_sessions_per_medium AS (
SELECT
  PARSE_DATE('%Y%m%d', event_date) AS event_date,
  traffic_source.medium AS medium,
  COUNT(DISTINCT CONCAT(user_pseudo_id, (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')))
  AS sessions
FROM `upheld-setting-420306.ga4_dataset.ga4_events`
GROUP BY PARSE_DATE('%Y%m%d', event_date), traffic_source.medium
),

medium_breakdown_by_period AS (
SELECT
  CASE WHEN event_date BETWEEN '2024-01-01' AND '2024-02-05' THEN 'Pre-anomaly'
  ELSE 'Anomaly'
  END AS period,
  medium,
  SUM(sessions) AS sessions
FROM daily_sessions_per_medium
GROUP BY 1, 2
)

SELECT
  *,
  CASE 
    WHEN period = 'Pre-anomaly' THEN 
      ROUND(100.00 * sessions / (SELECT SUM(sessions) FROM medium_breakdown_by_period WHERE period = 'Pre-anomaly'), 2) 
    WHEN period = 'Anomaly' THEN 
      ROUND(100.00 * sessions / (SELECT SUM(sessions) FROM medium_breakdown_by_period WHERE period = 'Anomaly'), 2)  
  END AS pct_session_share
FROM medium_breakdown_by_period
ORDER BY medium, period DESC

-- Task 3

WITH session_flags AS (
  SELECT
    CONCAT(user_pseudo_id, (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) AS session_id,
    CASE WHEN PARSE_DATE('%Y%m%d', event_date) BETWEEN '2024-01-01' AND '2024-02-05' THEN 'Pre-anomaly'
    ELSE 'Anomaly'
    END AS period, 
    MAX(CASE WHEN event_name = 'session_start'   THEN 1 ELSE 0 END) AS had_session_start,
    MAX(CASE WHEN event_name = 'view_item'        THEN 1 ELSE 0 END) AS had_view_item,
    MAX(CASE WHEN event_name = 'add_to_cart'      THEN 1 ELSE 0 END) AS had_add_to_cart,
    MAX(CASE WHEN event_name = 'begin_checkout'   THEN 1 ELSE 0 END) AS had_begin_checkout,
    MAX(CASE WHEN event_name = 'purchase'         THEN 1 ELSE 0 END) AS had_purchase
  FROM `upheld-setting-420306.ga4_dataset.ga4_events`
  GROUP BY 1,2
),

period_funnel AS (
  SELECT
    period,
    COUNT(session_id)       AS sessions,
    SUM(had_view_item)      AS view_item,
    SUM(had_add_to_cart)    AS add_to_cart,
    SUM(had_begin_checkout) AS begin_checkout,
    SUM(had_purchase)       AS purchase
  FROM session_flags
  GROUP BY period
),

pivoted AS (
  SELECT
    MAX(IF(period = 'Pre-anomaly',  sessions,  NULL)) AS n_session_start,
    MAX(IF(period = 'Pre-anomaly',  view_item,       NULL)) AS n_view_item,
    MAX(IF(period = 'Pre-anomaly',  add_to_cart,       NULL)) AS n_add_to_cart,
    MAX(IF(period = 'Pre-anomaly',  begin_checkout,       NULL)) AS n_begin_checkout,
    MAX(IF(period = 'Pre-anomaly',  purchase,       NULL)) AS n_purchase,

    MAX(IF(period = 'Anomaly', sessions,  NULL)) AS a_session_start,
    MAX(IF(period = 'Anomaly',  view_item,       NULL)) AS a_view_item,
    MAX(IF(period = 'Anomaly',  add_to_cart,       NULL)) AS a_add_to_cart,
    MAX(IF(period = 'Anomaly',  begin_checkout,       NULL)) AS a_begin_checkout,
    MAX(IF(period = 'Anomaly',  purchase,       NULL)) AS a_purchase,
  FROM period_funnel
),

step_funnel AS (
SELECT 'session_start' AS step,
  n_session_start  AS normal_period_sessions,
  a_session_start  AS anomaly_period_sessions,
  NULL             AS normal_dropoff_pct,   -- no previous step
  NULL             AS anomaly_dropoff_pct
FROM pivoted

UNION ALL

SELECT 'view_item',
  n_view_item,
  a_view_item,
  ROUND(100.0 * (n_session_start - n_view_item) / n_session_start, 2),
  ROUND(100.0 * (a_session_start - a_view_item) / a_session_start, 2)
FROM pivoted

UNION ALL

SELECT 'add_to_cart',
  n_add_to_cart,
  a_add_to_cart,
  ROUND(100.0 * (n_view_item - n_add_to_cart) / n_view_item, 2),
  ROUND(100.0 * (a_view_item - a_add_to_cart) / a_view_item, 2)
FROM pivoted

UNION ALL

SELECT 'begin_checkout',
  n_begin_checkout,
  a_begin_checkout,
  ROUND(100.0 * (n_add_to_cart - n_begin_checkout) / n_add_to_cart, 2),
  ROUND(100.0 * (a_add_to_cart - a_begin_checkout) / a_add_to_cart, 2)
FROM pivoted

UNION ALL

SELECT 'purchase',
  n_purchase,
  a_purchase,
  ROUND(100.0 * (n_begin_checkout - n_purchase) / n_begin_checkout, 2),
  ROUND(100.0 * (a_begin_checkout - a_purchase) / a_begin_checkout, 2)
FROM pivoted
)

SELECT
  *,
  ROUND(100.0*(anomaly_dropoff_pct - normal_dropoff_pct) / normal_dropoff_pct, 2) AS pct_change_in_dropoff
FROM step_funnel

-- Task 4
WITH session_flags AS (
  SELECT
    CONCAT(user_pseudo_id, (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) AS session_id,
    device.category AS device_category,
    CASE WHEN PARSE_DATE('%Y%m%d', event_date) BETWEEN '2024-01-01' AND '2024-02-05' THEN 'Pre-anomaly'
    ELSE 'Anomaly'
    END AS period, 
    MAX(CASE WHEN event_name = 'session_start'   THEN 1 ELSE 0 END) AS had_session_start,
    MAX(CASE WHEN event_name = 'view_item'        THEN 1 ELSE 0 END) AS had_view_item,
    MAX(CASE WHEN event_name = 'add_to_cart'      THEN 1 ELSE 0 END) AS had_add_to_cart,
    MAX(CASE WHEN event_name = 'begin_checkout'   THEN 1 ELSE 0 END) AS had_begin_checkout,
    MAX(CASE WHEN event_name = 'purchase'         THEN 1 ELSE 0 END) AS had_purchase
  FROM `upheld-setting-420306.ga4_dataset.ga4_events`
  GROUP BY 1,2,3
),

period_funnel AS (
  SELECT
    period,
    device_category,
    COUNT(session_id)       AS sessions,
    SUM(had_view_item)      AS view_item,
    SUM(had_add_to_cart)    AS add_to_cart,
    SUM(had_begin_checkout) AS begin_checkout,
    SUM(had_purchase)       AS purchase
  FROM session_flags
  GROUP BY period, device_category
),

pivoted AS (
  SELECT
    MAX(IF(period = 'Pre-anomaly' AND device_category = 'mobile',  sessions,  NULL)) AS n_mobile_session_start,
    MAX(IF(period = 'Pre-anomaly' AND device_category = 'mobile',  view_item,       NULL)) AS n_mobile_view_item,
    MAX(IF(period = 'Pre-anomaly' AND device_category = 'mobile',  add_to_cart,       NULL)) AS n_mobile_add_to_cart,
    MAX(IF(period = 'Pre-anomaly' AND device_category = 'mobile',  begin_checkout,       NULL)) AS n_mobile_begin_checkout,
    MAX(IF(period = 'Pre-anomaly' AND device_category = 'mobile',  purchase,       NULL)) AS n_mobile_purchase,

    MAX(IF(period = 'Pre-anomaly' AND device_category = 'desktop',  sessions,  NULL)) AS n_desktop_session_start,
    MAX(IF(period = 'Pre-anomaly' AND device_category = 'desktop',  view_item,       NULL)) AS n_desktop_view_item,
    MAX(IF(period = 'Pre-anomaly' AND device_category = 'desktop',  add_to_cart,       NULL)) AS n_desktop_add_to_cart,
    MAX(IF(period = 'Pre-anomaly' AND device_category = 'desktop',  begin_checkout,       NULL)) AS n_desktop_begin_checkout,
    MAX(IF(period = 'Pre-anomaly' AND device_category = 'desktop',  purchase,       NULL)) AS n_desktop_purchase,

    MAX(IF(period = 'Anomaly' AND device_category = 'mobile',  sessions,  NULL)) AS a_mobile_session_start,
    MAX(IF(period = 'Anomaly' AND device_category = 'mobile',  view_item,       NULL)) AS a_mobile_view_item,
    MAX(IF(period = 'Anomaly' AND device_category = 'mobile',  add_to_cart,       NULL)) AS a_mobile_add_to_cart,
    MAX(IF(period = 'Anomaly' AND device_category = 'mobile',  begin_checkout,       NULL)) AS a_mobile_begin_checkout,
    MAX(IF(period = 'Anomaly' AND device_category = 'mobile',  purchase,       NULL)) AS a_mobile_purchase,

    MAX(IF(period = 'Anomaly' AND device_category = 'desktop',  sessions,  NULL)) AS a_desktop_session_start,
    MAX(IF(period = 'Anomaly' AND device_category = 'desktop',  view_item,       NULL)) AS a_desktop_view_item,
    MAX(IF(period = 'Anomaly' AND device_category = 'desktop',  add_to_cart,       NULL)) AS a_desktop_add_to_cart,
    MAX(IF(period = 'Anomaly' AND device_category = 'desktop',  begin_checkout,       NULL)) AS a_desktop_begin_checkout,
    MAX(IF(period = 'Anomaly' AND device_category = 'desktop',  purchase,       NULL)) AS a_desktop_purchase,
  FROM period_funnel
),

step_funnel AS (
SELECT 'session_start' AS step, 'mobile' AS device, n_mobile_session_start, a_mobile_session_start, NULL, NULL FROM pivoted
UNION ALL
SELECT 'view_item', 'mobile', n_mobile_view_item, a_mobile_view_item,
  ROUND(100.0 * (n_mobile_session_start - n_mobile_view_item) / n_mobile_session_start, 2),
  ROUND(100.0 * (a_mobile_session_start - a_mobile_view_item) / a_mobile_session_start, 2)
FROM pivoted
UNION ALL
SELECT 'add_to_cart', 'mobile', n_mobile_add_to_cart, a_mobile_add_to_cart,
  ROUND(100.0 * (n_mobile_view_item - n_mobile_add_to_cart) / n_mobile_view_item, 2),
  ROUND(100.0 * (a_mobile_view_item - a_mobile_add_to_cart) / a_mobile_view_item, 2)
FROM pivoted
UNION ALL
SELECT 'begin_checkout', 'mobile', n_mobile_begin_checkout, a_mobile_begin_checkout,
  ROUND(100.0 * (n_mobile_add_to_cart - n_mobile_begin_checkout) / n_mobile_add_to_cart, 2),
  ROUND(100.0 * (a_mobile_add_to_cart - a_mobile_begin_checkout) / a_mobile_add_to_cart, 2)
FROM pivoted
UNION ALL
SELECT 'purchase', 'mobile', n_mobile_purchase, a_mobile_purchase,
  ROUND(100.0 * (n_mobile_begin_checkout - n_mobile_purchase) / n_mobile_begin_checkout, 2),
  ROUND(100.0 * (a_mobile_begin_checkout - a_mobile_purchase) / a_mobile_begin_checkout, 2)
FROM pivoted

UNION ALL

SELECT 'session_start', 'desktop', n_desktop_session_start, a_desktop_session_start, NULL, NULL FROM pivoted
UNION ALL
SELECT 'view_item', 'desktop', n_desktop_view_item, a_desktop_view_item,
  ROUND(100.0 * (n_desktop_session_start - n_desktop_view_item) / n_desktop_session_start, 2),
  ROUND(100.0 * (a_desktop_session_start - a_desktop_view_item) / a_desktop_session_start, 2)
FROM pivoted
UNION ALL
SELECT 'add_to_cart', 'desktop', n_desktop_add_to_cart, a_desktop_add_to_cart,
  ROUND(100.0 * (n_desktop_view_item - n_desktop_add_to_cart) / n_desktop_view_item, 2),
  ROUND(100.0 * (a_desktop_view_item - a_desktop_add_to_cart) / a_desktop_view_item, 2)
FROM pivoted
UNION ALL
SELECT 'begin_checkout', 'desktop', n_desktop_begin_checkout, a_desktop_begin_checkout,
  ROUND(100.0 * (n_desktop_add_to_cart - n_desktop_begin_checkout) / n_desktop_add_to_cart, 2),
  ROUND(100.0 * (a_desktop_add_to_cart - a_desktop_begin_checkout) / a_desktop_add_to_cart, 2)
FROM pivoted
UNION ALL
SELECT 'purchase', 'desktop', n_desktop_purchase, a_desktop_purchase,
  ROUND(100.0 * (n_desktop_begin_checkout - n_desktop_purchase) / n_desktop_begin_checkout, 2),
  ROUND(100.0 * (a_desktop_begin_checkout - a_desktop_purchase) / a_desktop_begin_checkout, 2)
FROM pivoted
)

SELECT
  *
FROM step_funnel
ORDER BY device, step

-- Task 5
-- the final query provides the base data. I have not included pct_calculations are they are better done in Excel

WITH raw_data AS (
SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    CONCAT(user_pseudo_id, (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) AS session_id,
    device.category AS device_category,
    CONCAT(traffic_source.source, '-', traffic_source.medium) AS source_medium,
    CASE WHEN PARSE_DATE('%Y%m%d', event_date) BETWEEN '2024-01-01' AND '2024-02-05' THEN 'Pre-anomaly'
    ELSE 'Anomaly'
    END AS period, 
    event_name,
    ecommerce
  FROM `upheld-setting-420306.ga4_dataset.ga4_events`
),

final_data AS (
  SELECT
    event_date,
    source_medium,
    device_category,
    period,
    COUNT(DISTINCT session_id) AS sessions,
    SUM(CASE WHEN event_name = 'view_item'        THEN 1 ELSE 0 END) AS view_item,
    SUM(CASE WHEN event_name = 'add_to_cart'      THEN 1 ELSE 0 END) AS add_to_cart,
    SUM(CASE WHEN event_name = 'begin_checkout'   THEN 1 ELSE 0 END) AS begin_checkout,
    SUM(CASE WHEN event_name = 'purchase'         THEN 1 ELSE 0 END) AS purchase,
    COALESCE(SUM(ecommerce.purchase_revenue), 0.0) AS purchase_revenue
  FROM raw_data
  GROUP BY event_date, source_medium, device_category, period
)

SELECT
  *
FROM final_data

