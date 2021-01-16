{% if not var("enable_football_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        alias='football_player_dim',
        unique_key='opta_stats_player_natural_key'
    )
}}
{% endif %}

with source as
(

  select * from {{ ref('int_football_player') }}

)

select
  {{ dbt_utils.surrogate_key(
    ['platform_celebrities_natural_key','opta_stats_player_natural_key']
  ) }} as football_player_pk,
  {{ dbt_utils.surrogate_key(
    ['opta_stats_player_natural_key']
  ) }} as opta_celebrities_pk,
  opta_stats_player_natural_key,
  case
    when platform_celebrities_natural_key is not null
    then
      {{ dbt_utils.surrogate_key(
        ['platform_celebrities_natural_key']
      ) }}
    else null
  end as platform_celebrities_fk,

  player_name,
  team_name,
  player_position,
  real_position,
  real_position_side,
  preferred_foot,
  shirt_number,
  join_ts,
  leave_ts

from source
left join {{ ref('wh_platform_celebrities_dim') }} celebrities on source.opta_stats_player_natural_key = celebrities.opta_celebrity_natural_key
