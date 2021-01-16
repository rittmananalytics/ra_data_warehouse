{% if not var("enable_football_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        alias='football_team_dim',
        unique_key='opta_stats_team_natural_key'
    )
}}
{% endif %}

with source as
(

  select * from {{ ref('int_football_team') }}

)

select
  {{ dbt_utils.surrogate_key(
    ['opta_stats_team_natural_key']
  ) }} as football_team_pk,
  opta_stats_team_natural_key,

  team_name

from source
