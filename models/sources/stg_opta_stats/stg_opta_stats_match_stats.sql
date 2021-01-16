{% if not var("enable_opta_stats_source") %}
  {{
      config(
          enabled=false
      )
  }}
{% endif %}

with source as (

  select * from {{ source('opta_stats','s_match_stats' ) }}
  where _fivetran_deleted is false and id != 10126

),

renamed as (

  select
    id as opta_stats_match_stats_natural_key,
    competition_id as opta_stats_competition_natural_key,
    team_home_ref as opta_stats_home_team_natural_key,
    team_away_ref as opta_stats_away_team_natural_key,
    winner_team_ref as opta_stats_winning_team_natural_key,
    cast(date as timestamp) as match_ts,
    lower(match_type) as match_type,
    lower(period) as match_status,
    lower(result_type) as match_result_type,
    cast(first_half_time as int64) as first_half_time,
    cast(second_half_time as int64) as second_half_time,
    cast(first_half_extra_time as int64) as first_half_extra_time,
    cast(second_half_extra_time as int64) as second_half_extra_time,
    cast(team_home_score as int64) as team_home_score,
    cast(team_away_score as int64) as team_away_score,
    cast(team_home_shootout_score as int64) as team_home_shootout_score,
    cast(team_away_shootout_score as int64) as team_away_shootout_score

  from source

)

select * from renamed
