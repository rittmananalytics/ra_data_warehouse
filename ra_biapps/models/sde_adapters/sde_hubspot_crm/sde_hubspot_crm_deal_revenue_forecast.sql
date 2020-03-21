{{
    config(
        materialized='table'
    )
}}
WITH daily_weighted_revenue as (
  SELECT
    *,
    (amount * probability) / nullif(contract_days,0) AS contract_daily_weighted_revenue,
    amount / contract_days AS contract_daily_full_revenue,
    (amount / contract_days) - ((amount * probability) / contract_days) AS contract_diff_daily_revenue
  FROM (
    SELECT
      *,
      TIMESTAMP_DIFF(end_date_ts,start_date_ts,DAY) AS contract_days
    FROM (
      SELECT
        current_dealname as dealname,
        deal_id,
          current_amount as amount,
          current_probability as probability,
          current_start_date_ts as start_date_ts,
          current_end_date_ts as end_date_ts
      FROM
        {{ ref('sde_hubspot_crm_deals') }}
      WHERE
          current_stage_label not in ('Closed Lost','Closed Won and Delivered')
      GROUP BY
        1,2,3,4,5,6))
),
months as (
  SELECT *
  FROM UNNEST(GENERATE_DATE_ARRAY('2019-01-10', '2024-01-01', INTERVAL 1 DAY)) day_ts
)
SELECT deal_id,
       date_trunc(day_ts,MONTH) as deal_forecast_month_ts,
       sum(revenue_days) as deal_forecast_revenue_days,
       sum(daily_weighted_revenue) as deal_forecast_weighted_amount,
       sum(daily_full_revenue) as deal_forecast_amount,
       sum(daily_diff_revenue) as deal_forecast_diff_amount
from (
  SELECT deal_id,
       day_ts,count(*) as revenue_days,
       sum(contract_daily_weighted_revenue) daily_weighted_revenue,
       sum(contract_daily_full_revenue) daily_full_revenue,
       sum(contract_diff_daily_revenue) daily_diff_revenue
  FROM months m
  JOIN daily_weighted_revenue d
  ON TIMESTAMP(m.day_ts) between d.start_date_ts and timestamp_sub(d.end_date_ts, interval 1 day)
  GROUP BY 1,2)
GROUP BY  1,2
