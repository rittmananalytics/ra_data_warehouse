{% if not var("enable_gamstop_source") or not var("enable_platform_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with merge_sources as
(

  {% if var("enable_gamstop_source") %}
    select * from {{ ref('stg_gamstop_check_history') }}
  {% endif %}

)

select * from merge_sources
