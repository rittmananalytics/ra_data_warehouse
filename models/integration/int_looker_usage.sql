{% if var("product_warehouse_usage_sources")  %}

with looker_usage_merge as
  (
    SELECT *
    FROM   {{ ref('stg_looker_usage_stats') }}
  )
select * from looker_usage_merge

{% else %} {{config(enabled=false)}} {% endif %}
