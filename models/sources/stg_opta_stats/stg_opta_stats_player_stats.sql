{% if not var("enable_opta_stats_source") %}
  {{
      config(
          enabled=false
      )
  }}
{% endif %}

with source as (

  select * from {{ source('opta_stats','s_player_stats' ) }}
  where _fivetran_deleted is false

),

renamed as (

  select
    id as opta_stats_player_stats_natural_key,
    match_id  as opta_stats_match_stats_natural_key,
    player_ref as opta_stats_player_natural_key,
    team_ref as opta_stats_team_natural_key,
    cast(created_on as timestamp) as player_stats_ts,
    lower(position) as player_stats_position,
    if(lower(status) = "start", true, false) as is_starter,
    cast(shirt_number as string) as shirt_number,
    cast(accurate_crosses as int64) as accurate_crosses,
    cast(ball_recoveries as int64) as ball_recoveries,
    cast(blocked_shots as int64) as blocked_shots,
    cast(clean_sheets as int64) as clean_sheets,
    cast(clearances as int64) as clearances,
    cast(crosses as int64) as crosses,
    cast(fouls as int64) as fouls,
    cast(giveaway_passes as int64) as giveaway_passes,
    cast(goal_assists as int64) as goal_assists,
    cast(goals as int64) as goals,
    cast(goals_conceded as int64) as goals_conceded,
    cast(interceptions as int64) as interceptions,
    cast(offsides as int64) as offsides,
    cast(own_goals as int64) as own_goals,
    cast(passes as int64) as passes,
    cast(penalties_conceded as int64) as penalties_conceded,
    cast(penalties_missed as int64) as penalties_missed,
    cast(penalty_goals as int64) as penalty_scored,
    cast(penalty_saved as int64) as penalty_saved,
    cast(saves as int64) as saves,
    cast(red_cards as int64) as red_cards,
    cast(second_yellow_cards as int64) as second_yellow_cards,
    cast(setpieces as int64) as set_pieces,
    cast(shots as int64) as shots,
    cast(shots_on_target as int64) as shots_on_target,
    cast(tackles as int64) as tackles,
    cast(was_fouled as int64) as was_fouled,
    cast(winning_goals as int64) as winning_goals,
    cast(wins as int64) as match_won,
    cast(looses as int64) as match_lost,
    cast(won_corners as int64) as won_corners,
    cast(won_tackles as int64) as won_tackles,
    cast(yellow_cards as int64) as yellow_cards,
    cast(minutes_played as int64) as minutes_played,
    cast(accurate_keeper_sweeper as int64) as accurate_keeper_sweeper,
    cast(accurate_long_balls as int64) as accurate_long_balls,
    cast(accurate_through_ball as int64) as accurate_through_ball,
    cast(aerial_won as int64) as aerial_duel_won,
    cast(big_chance_created as int64) as big_chance_created,
    cast(big_chance_missed as int64) as big_chance_missed,
    cast(gk_smother as int64) as goalkeeper_smother,
    cast(good_high_claim as int64) as goalkeeper_high_catch,
    cast(last_man_tackle as int64) as last_man_tackle,
    cast(punches as int64) as punches,
    cast(second_goal_assist as int64) as second_goal_assist,
    cast(total_att_assist as int64) as key_passes,
    cast(total_contest as int64) as total_contest,
    cast(won_contest as int64) as won_contest,
    cast(game_started as int64) as game_started

  from source

)

select * from renamed
