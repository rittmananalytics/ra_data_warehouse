{% if not var("enable_football_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        alias='football_match_stats_fact',
        unique_key='opta_stats_match_stats_natural_key'
    )
}}
{% endif %}

with source as
(

  select * from {{ ref('int_football_match_stats') }}
)

select
  {{ dbt_utils.surrogate_key(
    ['opta_stats_match_stats_natural_key']
  ) }} as football_match_stats_pk,
  opta_stats_match_stats_natural_key,
  {{ dbt_utils.surrogate_key(
    ['opta_stats_competition_natural_key']
  ) }} as football_competition_fk,
  opta_stats_competition_natural_key,
  {{ dbt_utils.surrogate_key(
    ['opta_stats_home_team_natural_key']
  ) }} as football_home_team_fk,
  opta_stats_home_team_natural_key,
  {{ dbt_utils.surrogate_key(
    ['opta_stats_away_team_natural_key']
  ) }} as football_away_team_fk,
  opta_stats_away_team_natural_key,
  {{ dbt_utils.surrogate_key(
    ['opta_stats_winning_team_natural_key']
  ) }} as football_winning_team_fk,
  opta_stats_winning_team_natural_key,

  match_ts,
  match_type,
  match_status,
  match_result_type,
  first_half_time,
  second_half_time,
  first_half_extra_time,
  second_half_extra_time,
  team_home_score,
  team_away_score,
  team_home_shootout_score,
  team_away_shootout_score

from source
