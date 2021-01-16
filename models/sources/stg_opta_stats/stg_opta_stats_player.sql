{% if not var("enable_opta_stats_source") %}
  {{
      config(
          enabled=false
      )
  }}
{% endif %}

with source as (

  select * from {{ source('opta_stats','s_player' ) }}
  where _fivetran_deleted is false

),

renamed as (

  select
    player_uid as opta_stats_player_natural_key,
    team_id as opta_stats_team_table_natural_key,
    lower(name) as player_name,
    lower(position) as player_position,
    lower(real_position) as real_position,
    lower(real_position_side) as real_position_side,
    lower(preferred_foot) as preferred_foot,
    cast(shirt_number as string) as shirt_number,
    cast(join_date as timestamp) as join_ts,
    cast(leave_date as timestamp) leave_ts

  from source

)

select * from renamed
