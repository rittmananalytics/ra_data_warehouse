{{ config(
    tags=["marketing"]
) }}

{{
  config(
    alias='marketing_user_sessions_fact',
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='marketing_user_session_pk',
    sort='starting_ts',
    dist='marketing_user_session_pk'
  )
}}

with sessions as
(

  select * from {{ ref('int_marketing_user_sessions') }}

  {% if is_incremental() %}
    where starting_ts > (select max(starting_ts) from {{ this }})
  {% endif %}

),

final as (

  select
    {{ dbt_utils.surrogate_key(
     ['user_session_sk']
    ) }} as marketing_user_session_pk,
    {{ dbt_utils.surrogate_key(
      ['platform_users_natural_key']
    ) }} as platform_user_fk,
    user_session_sk,
    platform_users_natural_key,

    starting_ts,
    ending_ts,
    duration_mins,
    sequence,
    landing_event_name,
    exit_event_name,
    initial_referrer,
    initial_referring_domain,
    referrer,
    referring_domain,
    search_engine,
    appsflyer_media_source,
    appsflyer_campaign,
    appsflyer_media_channel,
    appsflyer_media_ad_type,
    appsflyer_media_ad_set,
    appsflyer_affiliate_cost_model,
    appsflyer_affiliate_cost_currency,
    events_completed

  from sessions

)

select * from final
