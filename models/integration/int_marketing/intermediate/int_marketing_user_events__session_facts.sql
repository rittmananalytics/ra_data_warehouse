{{ config(
    tags=["marketing"]
) }}

{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='user_session_sk',
    sort='event_ts',
    dist='user_session_sk'
  )
}}

with source as (

  select * from {{ ref('int_marketing_user_events__sessionisation') }}

  {% if is_incremental() %}
    where event_ts > (select max(event_ts) from {{ this }})
  {% endif %}

),

session_boundaries as (

  select distinct

    platform_users_natural_key,
    user_session_sk,
    first_value (event_ts) over (partition by user_session_sk order by event_ts rows between unbounded preceding and unbounded following) as session_start_ts,
    last_value (event_ts) over (partition by user_session_sk order by event_ts rows between unbounded preceding and unbounded following) as session_end_ts,
    first_value (event_name) over (partition by user_session_sk order by event_ts rows between unbounded preceding and unbounded following) as session_landing_event_name,
    last_value  (event_name) over (partition by user_session_sk order by event_ts rows between unbounded preceding and unbounded following) as session_exit_event_name,
    first_value (initial_referrer) over (partition by user_session_sk order by event_ts rows between unbounded preceding and unbounded following) as initial_referrer,
    first_value (initial_referring_domain) over (partition by user_session_sk order by event_ts rows between unbounded preceding and unbounded following) as initial_referring_domain,
    first_value (referrer) over (partition by user_session_sk order by event_ts rows between unbounded preceding and unbounded following) as referrer,
    first_value (referring_domain) over (partition by user_session_sk order by event_ts rows between unbounded preceding and unbounded following) as referring_domain,
    first_value (search_engine) over (partition by user_session_sk order by event_ts rows between unbounded preceding and unbounded following) as search_engine,
    first_value (appsflyer_media_source) over (partition by user_session_sk order by event_ts rows between unbounded preceding and unbounded following) as appsflyer_media_source,
    first_value (appsflyer_campaign) over (partition by user_session_sk order by event_ts rows between unbounded preceding and unbounded following) as appsflyer_campaign,
    first_value (appsflyer_media_channel) over (partition by user_session_sk order by event_ts rows between unbounded preceding and unbounded following) as appsflyer_media_channel,
    first_value (appsflyer_media_ad_type) over (partition by user_session_sk order by event_ts rows between unbounded preceding and unbounded following) as appsflyer_media_ad_type,
    first_value (appsflyer_media_ad_set) over (partition by user_session_sk order by event_ts rows between unbounded preceding and unbounded following) as appsflyer_media_ad_set,
    first_value (appsflyer_affiliate_cost_model) over (partition by user_session_sk order by event_ts rows between unbounded preceding and unbounded following) as appsflyer_affiliate_cost_model,
    first_value (appsflyer_affiliate_cost_currency) over (partition by user_session_sk order by event_ts rows between unbounded preceding and unbounded following) as appsflyer_affiliate_cost_currency

  from
    source

),

session_events as (

  select
    platform_users_natural_key,
    user_session_sk,
    session_start_ts,
    session_end_ts,
    session_landing_event_name,
    session_exit_event_name,

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

    count(1) as events_in_session

  from session_boundaries

  {{ dbt_utils.group_by(n = 18)}}

),

session_duration as (

  select
    *,
    timestamp_diff(session_end_ts,session_start_ts,minute) as session_duration_mins

  from session_events

),

session_sequence as (

  select
    platform_users_natural_key,
    user_session_sk,
    session_start_ts,
    session_end_ts,
    session_landing_event_name,
    session_exit_event_name,
    session_duration_mins,
    events_in_session,

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

    row_number () over (partition by platform_users_natural_key order by min(session_start_ts)) as session_sequence_for_user,

  from session_duration

  {{ dbt_utils.group_by(n = 20)}}

)

select * from session_sequence
