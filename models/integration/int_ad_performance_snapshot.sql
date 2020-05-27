{% if not var("enable_facebook_ads_source") or (not var("enable_marketing_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with performance_snapshot as
  (
    SELECT *
    FROM   {{ ref('stg_facebook_ads_ad_performance_snapshot') }}
  )
select * from performance_snapshot
