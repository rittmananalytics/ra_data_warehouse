{% if not var("enable_affordability_source") or not var("enable_platform_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with merge_sources as (

  {% if var("enable_affordability_source") %}
    select * from {{ ref('stg_affordability_check_history') }}
  {% endif %}

)

select * from merge_sources
