{% if not var("enable_football_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        alias='football_competition_dim',
        unique_key='opta_stats_competition_natural_key'
    )
}}
{% endif %}

with source as
(

  select * from {{ ref('int_football_competition') }}

)

select
  {{ dbt_utils.surrogate_key(
    ['opta_stats_competition_natural_key']
  ) }} as football_competition_pk,
  opta_stats_competition_natural_key,

   competition_name,
   season_name

from source
