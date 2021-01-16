{{ config(
    tags=["marketing"]
) }}

{{ config(
    materialized='table',
    alias='marketing_user_journeys_xa',
    unique_key='marketing_user_journey_pk'
)}}

with users as (

  select distinct platform_user_fk from {{ ref('wh_marketing_user_events_fact') }}

),

event_installs as (

  select * from {{ ref('wh_marketing_user_events_fact') }}
  where event_name = 'install'

),

event_registrations as (

  select * from {{ ref('wh_marketing_user_events_fact') }}
  where event_name = 'regsuccess'

),

event_first_deposits as (

  select * from {{ ref('wh_marketing_user_events_fact') }}
  where event_name = 'ftd'

),

journey as (

  {% set install_partition_window = 'partition by event_installs.platform_user_fk order by event_installs.event_ts rows between unbounded preceding and unbounded following' %}
  {% set registration_partition_window = 'partition by event_registrations.platform_user_fk order by event_registrations.event_ts rows between unbounded preceding and unbounded following' %}
  {% set first_deposit_partition_window = 'partition by event_first_deposits.platform_user_fk order by event_first_deposits.event_ts rows between unbounded preceding and unbounded following' %}

  select distinct
    {{ dbt_utils.surrogate_key(
      ['platform_user_fk']
    ) }} as marketing_user_journey_pk,
    platform_user_fk,

    first_value(event_installs.marketing_user_event_pk) over ({{install_partition_window}}) as install_event_fk,
    first_value(event_installs.event_ts) over ({{install_partition_window}}) as install_event_ts,
    first_value(event_registrations.marketing_user_event_pk) over ({{registration_partition_window}}) as registration_event_fk,
    first_value(event_registrations.event_ts) over ({{registration_partition_window}}) as registration_event_ts,
    first_value(event_first_deposits.marketing_user_event_pk) over ({{first_deposit_partition_window}}) as first_deposit_event_fk,
    first_value(event_first_deposits.event_ts) over ({{first_deposit_partition_window}}) as first_deposit_event_ts

  from users
  left join event_installs using (platform_user_fk)
  left join event_registrations using (platform_user_fk)
  left join event_first_deposits using (platform_user_fk)

)

select * from journey
