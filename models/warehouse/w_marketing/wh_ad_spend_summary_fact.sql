{% if not var("enable_marketing_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
      alias='ad_spend_summary_fact'
    )
}}
{% endif %}
SELECT
  *
FROM (
  SELECT
    campaign_date,
    utm_source,
    utm_campaign,
    total_reported_cost,
    total_reported_impressions,
    total_reported_clicks,
    total_clicks,
    actual_vs_reported_clicks_pct,
    reported_cpc,
    actual_cpc,
    reported_ctr,
    actual_ctr,
    reported_cpm
  FROM
    {{ ref('wh_ad_campaign_performance_fact') }}
  WHERE
    utm_source = 'adwords'
    AND total_reported_clicks > 0
  UNION ALL
  SELECT
    campaign_date,
    utm_source,
    utm_campaign,
    total_reported_cost,
    total_reported_impressions,
    total_reported_clicks,
    total_clicks,
    actual_vs_reported_clicks_pct,
    reported_avg_cpc,
    actual_avg_cpc,
    reported_ctr,
    actual_ctr,
    reported_cpm
  FROM
    {{ ref('wh_ad_performance_fact') }}
  WHERE
    utm_source != 'adwords'
    AND total_reported_clicks > 0 )
ORDER BY
  1,
  2
