{% if not var("enable_football_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        alias='football_player_stats_fact',
        unique_key='opta_stats_player_stats_natural_key'
    )
}}
{% endif %}

with source as
(

  select * from {{ ref('int_football_player_stats') }}

)

select
  {{ dbt_utils.surrogate_key(
    ['opta_stats_player_stats_natural_key']
  ) }} as football_player_stats_pk,
  opta_stats_player_stats_natural_key,

  {{ dbt_utils.surrogate_key(
    ['platform_celebrities_natural_key','opta_stats_player_natural_key']
  ) }} as football_player_fk,
  opta_stats_player_natural_key,

  {{ dbt_utils.surrogate_key(
    ['opta_stats_team_natural_key']
  ) }} as football_team_fk,
  opta_stats_team_natural_key,


  {{ dbt_utils.surrogate_key(
    ['opta_stats_match_stats_natural_key']
  ) }} as football_match_stats_fk,

   player_stats_ts,
   player_stats_position,
   is_starter,
   shirt_number,
   accurate_crosses,
   ball_recoveries,
   blocked_shots,
   clean_sheets,
   clearances,
   crosses,
   fouls,
   giveaway_passes,
   goal_assists,
   goals,
   goals_conceded,
   interceptions,
   offsides,
   own_goals,
   passes,
   penalties_conceded,
   penalties_missed,
   penalty_scored,
   penalty_saved,
   saves,
   red_cards,
   second_yellow_cards,
   set_pieces,
   shots,
   shots_on_target,
   tackles,
   was_fouled,
   winning_goals,
   match_won,
   match_lost,
   won_corners,
   won_tackles,
   yellow_cards,
   minutes_played,
   accurate_keeper_sweeper,
   accurate_long_balls,
   accurate_through_ball,
   aerial_duel_won,
   big_chance_created,
   big_chance_missed,
   goalkeeper_smother,
   goalkeeper_high_catch,
   last_man_tackle,
   punches,
   second_goal_assist,
   key_passes,
   total_contest,
   won_contest,
   game_started

from source
left join {{ ref('wh_platform_celebrities_dim') }} celebrities on source.opta_stats_player_natural_key = celebrities.opta_celebrity_natural_key
