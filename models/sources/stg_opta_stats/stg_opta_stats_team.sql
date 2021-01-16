{% if not var("enable_opta_stats_source") %}
  {{
      config(
          enabled=false
      )
  }}
{% endif %}

with source as (

  select * from {{ source('opta_stats','s_team' ) }}
  where _fivetran_deleted is false

),

renamed as (

  select
    team_uid as opta_stats_team_natural_key,
    id as opta_stats_team_table_natural_key,
    lower(name) as team_name

  from source

)

select * from renamed
