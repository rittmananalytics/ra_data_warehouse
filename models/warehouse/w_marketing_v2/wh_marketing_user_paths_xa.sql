{{ config(
    tags=["marketing"]
) }}

{{ config(
    materialized='table',
    alias='marketing_user_paths_xa',
    unique_key='marketing_user_path_pk'
)}}

with events as (

  select * from {{ ref('wh_marketing_user_events_fact') }}

),

paths as (

  select
    {{ dbt_utils.surrogate_key(
      ['platform_user_fk']
    ) }} as marketing_user_path_pk,
    platform_user_fk,

    {% for i in range(1, 20) %}
      max(if(event_sequence_for_user = {{ i }}, event_name, NULL)) as event_{{i}},
      max(if(event_sequence_for_user = {{ i }}, event_properties, NULL)) as event_{{i}}_properties
      {% if not loop.last %},{% endif %}
    {% endfor %}

  from events

  group by 1, 2


)

select * from paths
