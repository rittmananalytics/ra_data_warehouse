{{ config(
    tags=["marketing"]
) }}

{{
  config(
    materialized='table',
    unique_key='mixpanel_events_natural_key',
    sort='event_ts'
  )
}}

with mixpanel_events as (

  select * from {{ ref('stg_mixpanel_events') }}

),

appsflyer_events as (

  select * from {{ ref('stg_appsflyer_events') }}

),

final as (

  select distinct
    mixpanel_events.mixpanel_events_natural_key,
    mixpanel_events.platform_users_natural_key,

    mixpanel_events.event_ts,

    mixpanel_events.initial_referrer,
    mixpanel_events.initial_referring_domain,
    mixpanel_events.referrer,
    mixpanel_events.referring_domain,
    mixpanel_events.search_engine,
    appsflyer_events.appsflyer_media_source,
    appsflyer_events.appsflyer_campaign,
    appsflyer_events.appsflyer_media_channel,
    appsflyer_events.appsflyer_media_ad_type,
    appsflyer_events.appsflyer_media_ad_set,
    appsflyer_events.appsflyer_affiliate_cost_model,
    appsflyer_events.appsflyer_affiliate_cost_currency,
    mixpanel_events.event_name,
    mixpanel_events.event_properties

  from mixpanel_events
  left join appsflyer_events using(platform_users_natural_key, event_name)

)

select * from final
where platform_users_natural_key is not null
