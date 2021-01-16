{{ config(
    tags=["marketing"]
) }}

{{
  config(
    alias='marketing_user_events_fact',
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='marketing_user_event_pk',
    sort='event_ts',
    dist='marketing_user_event_pk'
  )
}}

with events as (

  select * from {{ ref('int_marketing_user_events') }}

  {% if is_incremental() %}
  where event_ts > (select max(event_ts) from {{ this }})
  {% endif %}

),

final as (

  select
    {{ dbt_utils.surrogate_key(
      ['mixpanel_events_natural_key']
    ) }} as marketing_user_event_pk,
    {{ dbt_utils.surrogate_key(
      ['platform_users_natural_key']
    ) }} as platform_user_fk,
    {{ dbt_utils.surrogate_key(
      ['user_session_sk']
    ) }} as marketing_user_session_fk,
    mixpanel_events_natural_key,
    platform_users_natural_key,
    user_session_sk,

    event_sequence_for_session,
    event_sequence_for_user,
    event_name,
    event_properties,
    event_ts

  from events

)

select * from final
