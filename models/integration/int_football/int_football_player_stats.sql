{% if not var("enable_opta_stats_source") or not var("enable_football_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with merge_sources as
(

  {% if var("enable_opta_stats_source") %}
    select * from {{ ref('stg_opta_stats_player_stats') }}
  {% endif %}

)

select * from merge_sources
