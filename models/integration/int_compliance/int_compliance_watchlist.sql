{% if not var("enable_compliance_watchlist_source") or not var("enable_compliance_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with merge_sources as
(

  {% if var("enable_compliance_watchlist_source") %}
    select * from {{ ref('stg_compliance_watchlist') }}
  {% endif %}

)

select * from merge_sources
