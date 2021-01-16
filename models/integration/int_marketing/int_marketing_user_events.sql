{{ config(
    tags=["marketing"]
) }}

with source as (

  select * from {{ ref('int_marketing_user_events__sessionisation') }}

),

events as (

  select

    mixpanel_events_natural_key,
    platform_users_natural_key,
    user_session_sk,

    row_number () over (
      partition by user_session_sk
      order by event_ts
    ) as event_sequence_for_session,
    row_number () over (
      partition by platform_users_natural_key
      order by event_ts
    ) as event_sequence_for_user,
    event_name,
    event_properties,
    event_ts

  from source

)

select * from events
