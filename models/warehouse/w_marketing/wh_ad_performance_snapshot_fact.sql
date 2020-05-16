{% if not var("enable_facebook_ads_source") or (not var("enable_marketing_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        alias='ad_performance_snapshot_fact',
        materialized='incremental'
    )
}}
{% endif %}

with ads_dim as (
      select *
      from {{ ref('wh_ads_dim') }}
),
adsets_dim as (
      select *
      from {{ ref('wh_adsets_dim') }}
),
ad_campaigns_dim AS
  (
  SELECT *
  FROM   {{ ref('wh_ad_campaigns_dim') }}
),
ad_performance_snapshot AS
  (
    SELECT *
    FROM   {{ ref('int_ad_performance_snapshot') }}
  )
SELECT

    GENERATE_UUID() as ad_performance_snapshot_pk,
    a.ad_pk,
    d.adset_pk,
    c.campaign_pk,
    s.* except (ad_id,
               adset_id,
               campaign_id)
FROM
   ad_performance_snapshot s
JOIN ads_dim a
   ON s.ad_id = a.ad_id
JOIN ad_campaigns_dim c
   ON s.campaign_id = c.campaign_id
JOIN adsets_dim d
   ON s.adset_id = d.adset_id

{% if is_incremental() %}
     -- this filter will only be applied on an incremental run
     where ad_snapshot_ts > (select max(ad_snapshot_ts) from {{ this }})
{% endif %}
