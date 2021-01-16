{% if not var("enable_fv_media_source") or not var("enable_football_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with merge_sources as
(

  {% if var("enable_fv_media_source") %}
    select * from {{ ref('stg_fv_media_articles') }}
  {% endif %}

)

select * from merge_sources
