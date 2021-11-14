WITH
  numbers AS (
  SELECT
    blended_user_id,
    SUM(count_conversions) AS count_conversions,
    SUM( count_order_conversions) AS count_order_conversions,
    SUM(count_first_order_conversions) count_first_order_conversions,
    SUM(count_registration_conversions) count_registration_conversions,
    SUM(count_repeat_order_conversions) count_repeat_order_conversions,
    SUM(first_click_attrib_pct) first_click_attrib_pct,
    SUM(first_non_direct_click_attrib_pct) first_non_direct_click_attrib_pct,
    SUM(first_paid_click_attrib_pct) first_paid_click_attrib_pct,
    SUM(last_click_attrib_pct) last_click_attrib_pct,
    SUM(last_non_direct_click_attrib_pct) last_non_direct_click_attrib_pct,
    SUM(last_paid_click_attrib_pct) last_paid_click_attrib_pct,
    SUM(even_click_attrib_pct) even_click_attrib_pct,
    SUM(time_decay_attrib_pct) time_decay_attrib_pct,
    SUM(user_registration_first_click_attrib_conversions) user_registration_first_click_attrib_conversions,
    SUM(user_registration_first_non_direct_click_attrib_conversions) user_registration_first_non_direct_click_attrib_conversions,
    SUM(user_registration_first_paid_click_attrib_conversions) user_registration_first_paid_click_attrib_conversions,
    SUM(user_registration_last_click_attrib_conversions) user_registration_last_click_attrib_conversions,
    SUM(user_registration_last_non_direct_click_attrib_conversions) user_registration_last_non_direct_click_attrib_conversions,
    SUM(user_registration_last_paid_click_attrib_conversions) user_registration_last_paid_click_attrib_conversions,
    SUM(user_registration_even_click_attrib_conversions) user_registration_even_click_attrib_conversions,
    SUM(user_registration_time_decay_attrib_conversions) user_registration_time_decay_attrib_conversions,
    SUM(first_order_first_click_attrib_conversions) first_order_first_click_attrib_conversions,
    SUM(first_order_first_non_direct_click_attrib_conversions) first_order_first_non_direct_click_attrib_conversions,
    SUM(first_order_last_click_attrib_conversions) first_order_last_click_attrib_conversions,
    SUM(first_order_last_non_direct_click_attrib_conversions) first_order_last_non_direct_click_attrib_conversions,
    SUM(first_order_last_paid_click_attrib_conversions) first_order_last_paid_click_attrib_conversions,
    SUM(first_order_even_click_attrib_conversions) first_order_even_click_attrib_conversions,
    SUM(first_order_time_decay_attrib_conversions) first_order_time_decay_attrib_conversions,
    SUM(repeat_order_first_click_attrib_conversions) repeat_order_first_click_attrib_conversions,
    SUM(repeat_order_first_paid_click_attrib_conversions) repeat_order_first_paid_click_attrib_conversions,
    SUM(repeat_order_last_click_attrib_conversions) repeat_order_last_click_attrib_conversions,
    SUM(repeat_order_last_non_direct_click_attrib_conversions) repeat_order_last_non_direct_click_attrib_conversions,
    SUM(repeat_order_last_paid_click_attrib_conversions) repeat_order_last_paid_click_attrib_conversions,
    SUM(repeat_order_even_click_attrib_conversions) repeat_order_even_click_attrib_conversions,
    SUM(repeat_order_time_decay_attrib_conversions) repeat_order_time_decay_attrib_conversions,
    SUM(first_order_total_revenue) first_order_total_revenue,
    SUM(first_order_first_click_attrib_revenue) first_order_first_click_attrib_revenue,
    SUM(first_order_first_non_direct_click_attrib_revenue) first_order_first_non_direct_click_attrib_revenue,
    SUM(first_order_first_paid_click_attrib_revenue) first_order_first_paid_click_attrib_revenue,
    SUM(first_order_last_click_attrib_revenue) first_order_last_click_attrib_revenue,
    SUM(first_order_last_non_direct_click_attrib_revenue) first_order_last_non_direct_click_attrib_revenue,
    SUM(first_order_last_paid_click_attrib_revenue) first_order_last_paid_click_attrib_revenue,
    SUM(first_order_even_click_attrib_revenue) first_order_even_click_attrib_revenue,
    SUM(first_order_time_decay_attrib_revenue) first_order_time_decay_attrib_revenue,
    SUM(repeat_order_total_revenue) repeat_order_total_revenue,
    SUM(repeat_order_first_click_attrib_revenue) repeat_order_first_click_attrib_revenue,
    SUM(repeat_order_first_non_direct_click_attrib_revenue) repeat_order_first_non_direct_click_attrib_revenue,
    SUM(repeat_order_first_paid_click_attrib_revenue ) repeat_order_first_paid_click_attrib_revenue,
    SUM(repeat_order_last_click_attrib_revenue) repeat_order_last_click_attrib_revenue,
    SUM(repeat_order_last_non_direct_click_attrib_revenue) repeat_order_last_non_direct_click_attrib_revenue,
    SUM(repeat_order_last_paid_click_attrib_revenue) repeat_order_last_paid_click_attrib_revenue,
    SUM(repeat_order_even_click_attrib_revenue) repeat_order_even_click_attrib_revenue,
    SUM(repeat_order_time_decay_attrib_revenue) repeat_order_time_decay_attrib_revenue
  FROM
    {{ ref('attribution_fact') }}
  GROUP BY
    1),
  totals AS (
  SELECT
    SUM(count_conversions) AS count_conversions,
    SUM(count_first_order_conversions) AS count_first_order_conversions,
    SUM(count_repeat_order_conversions) AS count_repeat_order_conversions,
    SUM(first_order_total_revenue) AS first_order_total_revenue,
    SUM(repeat_order_first_click_attrib_revenue) AS repeat_order_first_click_attrib_revenue,
    SUM(repeat_order_last_click_attrib_revenue) AS repeat_order_last_click_attrib_revenue,
    SUM(repeat_order_even_click_attrib_revenue) AS repeat_order_even_click_attrib_revenue,
    SUM(repeat_order_time_decay_attrib_revenue) AS repeat_order_time_decay_attrib_revenue,
    SUM(first_order_first_click_attrib_revenue) AS first_order_first_click_attrib_revenue,
    SUM(first_order_last_click_attrib_revenue) AS first_order_last_click_attrib_revenue,
    SUM(first_order_even_click_attrib_revenue) AS first_order_even_click_attrib_revenue,
    SUM(first_order_time_decay_attrib_revenue) AS first_order_time_decay_attrib_revenue,
    SUM(repeat_order_total_revenue) AS repeat_order_total_revenue,
    SUM(count_registration_conversions) AS count_registration_conversions,
    SUM(user_registration_first_click_attrib_conversions) AS user_registration_first_click_attrib_conversions,
    SUM(user_registration_last_click_attrib_conversions) AS user_registration_last_click_attrib_conversions,
    SUM(user_registration_even_click_attrib_conversions) AS user_registration_even_click_attrib_conversions,
    SUM(user_registration_time_decay_attrib_conversions) AS user_registration_time_decay_attrib_conversions,
    SUM(first_order_first_click_attrib_conversions) AS first_order_first_click_attrib_conversions,
    SUM(first_order_last_click_attrib_conversions) AS first_order_last_click_attrib_conversions,
    SUM(first_order_even_click_attrib_conversions) AS first_order_even_click_attrib_conversions,
    SUM(first_order_time_decay_attrib_conversions) AS first_order_time_decay_attrib_conversions,
    SUM(repeat_order_first_click_attrib_conversions) AS repeat_order_first_click_attrib_conversions,
    SUM(repeat_order_last_click_attrib_conversions) AS repeat_order_last_click_attrib_conversions,
    SUM(repeat_order_even_click_attrib_conversions) AS repeat_order_even_click_attrib_conversions,
    SUM(repeat_order_time_decay_attrib_conversions) AS repeat_order_time_decay_attrib_conversions
  FROM
    numbers)
SELECT * FROM (
SELECT
  'conversion counts' AS test,
  (count_conversions-count_registration_conversions-count_first_order_conversions-count_repeat_order_conversions)=0 AS pass
FROM
  totals
UNION ALL
SELECT
  'first_order_revenue' AS test,
  CASE
    WHEN (first_order_total_revenue = ROUND(first_order_first_click_attrib_revenue)) AND (first_order_total_revenue = ROUND(first_order_last_click_attrib_revenue)) AND (first_order_total_revenue = ROUND(first_order_even_click_attrib_revenue)) AND (first_order_total_revenue = ROUND(first_order_time_decay_attrib_revenue)) THEN TRUE
  ELSE
  FALSE
END
  AS pass
FROM
  totals
UNION ALL
SELECT
  'repeat_order_revenue' AS test,
  CASE
    WHEN (repeat_order_total_revenue = ROUND(repeat_order_first_click_attrib_revenue)) AND (repeat_order_total_revenue = ROUND(repeat_order_last_click_attrib_revenue)) AND (repeat_order_total_revenue = ROUND(repeat_order_even_click_attrib_revenue)) AND (repeat_order_total_revenue = ROUND(repeat_order_time_decay_attrib_revenue)) THEN TRUE
  ELSE
  FALSE
END
  AS pass
FROM
  totals)
WHERE pass = false
