{% if not var("enable_looker_usage_source") or (not var("enable_product_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with looker_usage_merge as
  (
    SELECT *
    FROM   {{ ref('stg_looker_usage_usage_stats') }}
  )
select * from looker_usage_merge
