{% if var('marketing_warehouse_ad_group_sources') %}

with ad_group_performance as
  (
    SELECT
      date_day          as ad_campaign_serve_ts,
      ad_group_id       as ad_group_id,
      account_id        as ad_account_id,
      platform          as ad_network,
      sum(clicks)       as ad_campaign_total_clicks,
      sum(impressions)  as ad_campaign_total_impressions,
      sum(spend)        as ad_campaign_total_cost
    FROM
      {{ ref('int_ad_reporting') }}
    GROUP BY
      1,2,3,4
  )
select * from ad_group_performance

{% else %}

{{
    config(
        enabled=false
    )
}}


{% endif %}
