{% if not var("enable_football_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        alias='football_pb_stats_xa',
        unique_key='football_player_stats_pk'
    )
}}
{% endif %}

with player_stats as (

    select  *  from {{ ref('wh_football_player_stats_fact') }}

),

player as (

    select * from {{ ref('wh_football_player_dim') }}
),

celebrities as (

    select * from {{ ref('wh_platform_celebrities_dim') }}

),

team as (

    select * from {{ ref('wh_football_team_dim') }}

),

match_stats as (

    select * from {{ ref('wh_football_match_stats_fact') }}

),

competition as (

    select * from {{ ref('wh_football_competition_dim') }}

),

merge_sources as (

  select
    player_stats.*,
    match_stats.opta_stats_match_stats_natural_key,
    match_stats.match_ts,
    player.player_name,
    player.player_position,
    celebrities.opta_celebrity_natural_key,
    celebrities.platform_celebrities_natural_key,
    team.team_name,
    competition.opta_stats_competition_natural_key,
    competition.competition_name

  from player_stats
  left join player
    using (opta_stats_player_natural_key)
    -- on player_stats.football_player_fk = player.football_player_pk
  left join celebrities
    on player.platform_celebrities_fk = celebrities.platform_celebrity_pk
  left join team
    on player_stats.football_team_fk = team.football_team_pk
  left join match_stats
    on player_stats.football_match_stats_fk = match_stats.football_match_stats_pk
  left join competition
    on match_stats.football_competition_fk = competition.football_competition_pk

),

pb_multipliers as (

select

  football_player_stats_pk,
  opta_stats_player_stats_natural_key,
  football_player_fk,
  opta_stats_player_natural_key,
  football_team_fk,
  opta_stats_team_natural_key,
  football_match_stats_fk,
  opta_stats_competition_natural_key,
  opta_stats_match_stats_natural_key,

  match_ts,
  player_name,
  team_name,
  competition_name,
  player_position,
  player_stats_position,

  (sum(winning_goals)  *  35) winning_goals_multiplier,
  (sum(goals)  *  45) goals_multiplier,
  (sum(match_won) * 18) match_won_multiplier,
  (sum(match_lost) * -15) match_lost_multiplier,
  (sum(last_man_tackle) * 20) last_man_tackle_multiplier,
  (sum(goal_assists) * 20) goal_assists_multiplier,
  (sum(shots_on_target) * 5) shots_on_target_multiplier,
  (sum(blocked_shots) * case when player_position='goalkeeper' then 20 else 5 end) blocked_shots_multiplier,
  (sum(won_tackles) * 4) won_tackles_multiplier,
  (sum(interceptions) * 5) interceptions_multiplier,
  (sum(saves) * case when player_position='goalkeeper' then 10 else 0 end) saves_multiplier,
  (sum(ball_recoveries) * 3) ball_recoveries_multiplier,
  (sum(accurate_crosses) * 4) accurate_crosses_multiplier,
  (sum(was_fouled) * 4) was_fouled_multiplier,
  (sum(tackles) * 3) tackles_multiplier,
  (sum(won_corners) * 5) won_corners_multiplier,
  (sum(clearances) * 3) clearances_multiplier,
  (sum(shots) * 3) shots_multiplier,
  (sum(crosses) * 3) crosses_multiplier,
  (sum(passes) * 1) passes_multiplier,
  (sum(clean_sheets) * case when player_position='defender' then 25 when player_position='goalkeeper' then 40 else 0 end) clean_sheets_multiplier,
  (sum(penalty_saved) * case when player_position='goalkeeper' then 45 else 0 end) penalty_saved_multiplier,
  (sum(giveaway_passes) * -3) giveaway_passes_multiplier,
  (sum(fouls) * -5) fouls_multiplier,
  (sum(offsides) * -5) offsides_multiplier,
  (sum(yellow_cards) * -5) yellow_cards_multiplier,
  (sum(second_yellow_cards) * -10) second_yellow_cards_multiplier,
  (sum(goals_conceded) * -5) goals_conceded_multiplier,
  (sum(penalties_conceded) * -15) penalties_conceded_multiplier,
  (sum(penalties_missed) * -20) penalties_missed_multiplier,
  (sum(own_goals) * -30) own_goals_multiplier,
  (sum(red_cards) * -30) red_cards_multiplier,
  (sum(big_chance_missed) * -10) big_chance_missed_multiplier,
  (sum(goalkeeper_smother) * case when player_position='goalkeeper' then 3 else 0 end) goalkeeper_smother_multiplier,
  (sum(goalkeeper_high_catch) * case when player_position='goalkeeper' then 2 else 0 end) goalkeeper_high_catch_multiplier,
  (sum(punches) * case when player_position='goalkeeper' then 2 else 0 end) punches_multiplier,
  (sum(accurate_keeper_sweeper) * case when player_position='goalkeeper' then 3 else 0 end) accurate_keeper_sweeper_multiplier,
  (sum(accurate_long_balls) * 2) accurate_long_balls_multiplier,
  (sum(accurate_through_ball) * 3) accurate_through_ball_multiplier,
  (sum(big_chance_created) * 3) big_chance_created_multiplier,
  (sum(aerial_duel_won) * 2) aerial_duel_won_multiplier,
  (sum(total_contest)) total_contest_multiplier,
  (sum(won_contest) * 2) won_contest_multiplier,
  (sum(key_passes) * 6) key_passes_multiplier,
  (sum(second_goal_assist)  *  3) second_goal_assist_multiplier

  from merge_sources

  {{ dbt_utils.group_by(n=15) }}

),

pb_score as (

  select
    *,
    winning_goals_multiplier +
    goals_multiplier +
    match_won_multiplier +
    match_lost_multiplier +
    last_man_tackle_multiplier +
    goal_assists_multiplier +
    shots_on_target_multiplier +
    blocked_shots_multiplier +
    won_tackles_multiplier +
    interceptions_multiplier +
    saves_multiplier +
    ball_recoveries_multiplier +
    accurate_crosses_multiplier +
    was_fouled_multiplier +
    tackles_multiplier +
    won_corners_multiplier +
    clearances_multiplier +
    shots_multiplier +
    crosses_multiplier +
    passes_multiplier +
    clean_sheets_multiplier +
    penalty_saved_multiplier +
    giveaway_passes_multiplier +
    fouls_multiplier +
    offsides_multiplier +
    yellow_cards_multiplier +
    second_yellow_cards_multiplier +
    goals_conceded_multiplier +
    penalties_conceded_multiplier +
    penalties_missed_multiplier +
    own_goals_multiplier +
    red_cards_multiplier +
    big_chance_missed_multiplier +
    goalkeeper_smother_multiplier +
    goalkeeper_high_catch_multiplier +
    punches_multiplier +
    accurate_keeper_sweeper_multiplier +
    accurate_long_balls_multiplier +
    accurate_through_ball_multiplier +
    big_chance_created_multiplier +
    aerial_duel_won_multiplier +
    total_contest_multiplier +
    won_contest_multiplier +
    key_passes_multiplier +
    second_goal_assist_multiplier as match_day_score

  from pb_multipliers

)

select * from pb_score
