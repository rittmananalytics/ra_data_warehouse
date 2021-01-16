{{ config(
    tags=["marketing"]
) }}

{{ config(
    materialized='table',
    alias='marketing_session_paths_xa',
    unique_key='marketing_session_path_pk'
)}}

with events as (

  select * from {{ ref('wh_marketing_user_events_fact') }}

),

paths as (

  select
    {{ dbt_utils.surrogate_key(
      ['platform_user_fk', 'marketing_user_session_fk']
    ) }} as marketing_session_path_pk,
    platform_user_fk,
    marketing_user_session_fk,

    {% for i in range(1, 20) %}
      max(if(event_sequence_for_session = {{ i }}, event_name, NULL)) as event_{{i}},
      max(if(event_sequence_for_session = {{ i }}, event_properties, NULL)) as event_{{i}}_properties
      {% if not loop.last %},{% endif %}
    {% endfor %}

  from events

  group by 1, 2, 3


)

select * from paths
