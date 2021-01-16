{% if not var("enable_opta_stats_source") %}
  {{
      config(
          enabled=false
      )
  }}
{% endif %}

with source as (

  select * from {{ source('opta_stats','s_competition' ) }}
  where _fivetran_deleted is false

),

renamed as (

  select
    id as opta_stats_competition_natural_key,
    lower(name) as competition_name,
    lower(season_name) as season_name,

  from source

)

select * from renamed
