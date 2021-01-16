{% if not var("enable_opta_stats_source") or not var("enable_football_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with player as
(

    select * from {{ ref('stg_opta_stats_player') }}

),

team as (

    select * from {{ ref('stg_opta_stats_team') }}

),

merge_sources as (

  select
    opta_stats_player_natural_key,
    player_name,
    team_name,
    player_position,
    real_position,
    real_position_side,
    preferred_foot,
    shirt_number,
    join_ts,
    leave_ts
  from player
  left join team using (opta_stats_team_table_natural_key)

)

select * from merge_sources
