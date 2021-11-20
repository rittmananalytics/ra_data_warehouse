{% if var('marketing_warehouse_ad_campaign_sources') %}


with ad_campaign_performance as
  (
    SELECT
      date_day          AS ad_campaign_serve_ts,
      campaign_id       AS ad_campaign_id,
      account_id        AS ad_account_id,
      platform          AS ad_network,
      sum(clicks)       AS ad_campaign_total_clicks,
      sum(impressions)  AS ad_campaign_total_impressions,
      sum(spend)        AS ad_campaign_total_cost
    FROM
      {{ ref('int_ad_reporting') }}
    GROUP BY
      1,2,3,4
  )
SELECT * FROM ad_campaign_performance

{% else %}

{{
    config(
        enabled=false
    )
}}


{% endif %}
