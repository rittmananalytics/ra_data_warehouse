{{ config(
    tags=["marketing"]
) }}

with source as (

  select

    user_session_sk,
    platform_users_natural_key,

    session_start_ts as starting_ts,
    session_end_ts as ending_ts,

    session_duration_mins as duration_mins,
    session_sequence_for_user as sequence,
    session_landing_event_name as landing_event_name,
    session_exit_event_name as exit_event_name,
    events_in_session as events_completed,
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
    appsflyer_affiliate_cost_currency

  from {{ ref('int_marketing_user_events__session_facts') }}

)

select * from source
