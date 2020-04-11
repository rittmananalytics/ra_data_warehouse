{{
    config(
        alias='deals_revenue_forecast_fact'
    )
}}
WITH daily_weighted_revenue as (
  SELECT
    *,
    (deal_amount * deal_probability_pct) / nullif(deal_duration_days,0) AS contract_daily_weighted_revenue,
    deal_amount / deal_duration_days AS contract_daily_full_revenue,
    (deal_amount / deal_duration_days) - ((deal_amount * deal_probability_pct) / deal_duration_days) AS contract_diff_daily_revenue
  FROM (
        SELECT
        deal_name,
        deal_pk,
        deal_amount,
        deal_probability_pct,
        deal_delivery_start_ts,
        deal_delivery_end_date_ts,
        deal_duration_days
      FROM
        {{ ref('sil_deals_fact') }}
      WHERE
          deal_stage_label not in ('Closed Lost','Closed Won and Delivered')
      )
      )
,
months as (
  SELECT *
  FROM UNNEST(GENERATE_DATE_ARRAY('2019-01-10', '2024-01-01', INTERVAL 1 DAY)) day_ts
)
SELECT deal_pk,
       date_trunc(day_ts,MONTH) as deal_forecast_month_ts,
       sum(revenue_days) as deal_forecast_revenue_days,
       sum(daily_weighted_revenue) as deal_forecast_weighted_amount,
       sum(daily_full_revenue) as deal_forecast_amount,
       sum(daily_diff_revenue) as deal_forecast_diff_amount
from (
  SELECT deal_pk,
       day_ts,count(*) as revenue_days,
       sum(contract_daily_weighted_revenue) daily_weighted_revenue,
       sum(contract_daily_full_revenue) daily_full_revenue,
       sum(contract_diff_daily_revenue) daily_diff_revenue
  FROM months m
  JOIN daily_weighted_revenue d
  ON TIMESTAMP(m.day_ts) between d.deal_delivery_start_ts and timestamp_sub(d.deal_delivery_end_date_ts, interval 1 day)
  GROUP BY 1,2)
GROUP BY  1,2
