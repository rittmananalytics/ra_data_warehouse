with source as (

  select * from {{ source('appsflyer', 's_events') }}
  where date(event_time) >= '2020-05-01'

),

events as (

  select
    cast(_fivetran_id as string) as appsflyer_events_natural_key,
    cast(appsflyer_id as string) as appsflyer_users_natural_key,
    cast(
      last_value(customer_user_id) over (
        partition by appsflyer_id order by event_time rows between unbounded preceding and unbounded following
    ) as string) as platform_users_natural_key,
    cast(af_siteid as string) as appsflyer_publisher_natural_key,
    cast(af_c_id as string) as appsflyer_media_campaign_natural_key,
    cast(af_adset_id as string) as appsflyer_media_adset_natural_key,
    cast(af_ad_id as string) as appsflyer_media_ad_natural_key,

    event_time as event_ts,
    install_time as user_app_install_ts,
    attributed_touch_time as touch_ts,

    idfa
    event_value, 
    lower(platform) as user_platform,
    lower(attributed_touch_type) as touch_type,
    lower(http_referrer) as original_http_request,
    lower(original_url) as original_url_clicked,
    lower(media_source) as appsflyer_media_source,
    lower(campaign) as appsflyer_campaign,
    lower(af_channel) as appsflyer_media_channel,
    lower(af_ad_type) as appsflyer_media_ad_type,
    lower(af_adset) as appsflyer_media_ad_set,
    lower(af_cost_model) as appsflyer_affiliate_cost_model,
    lower(af_cost_currency) as appsflyer_affiliate_cost_currency,
    lower(event_type) as event_type,
    replace(replace(trim(lower(event_name)), ' ', ''), '_', '') as event_name,

    cast(af_cost_value as numeric) as appsflyer_affiliate_cost

  from source

  where appsflyer_id is not null

),

dedup as (

  select distinct
    platform_users_natural_key,
    event_name, 
    
    first_value(appsflyer_media_source) over (
      partition by platform_users_natural_key, event_name 
      order by event_ts 
      rows between unbounded preceding and unbounded following
    ) as appsflyer_media_source,
    first_value(appsflyer_campaign) over (
      partition by platform_users_natural_key, event_name 
      order by event_ts 
      rows between unbounded preceding and unbounded following
    ) as appsflyer_campaign,
    first_value(appsflyer_media_channel) over (
      partition by platform_users_natural_key, event_name 
      order by event_ts 
      rows between unbounded preceding and unbounded following
    ) as appsflyer_media_channel,
    first_value(appsflyer_media_ad_set) over (
      partition by platform_users_natural_key, event_name 
      order by event_ts 
      rows between unbounded preceding and unbounded following
    ) as appsflyer_media_ad_set,
    first_value(appsflyer_media_ad_type) over (
      partition by platform_users_natural_key, event_name 
      order by event_ts 
      rows between unbounded preceding and unbounded following
    ) as appsflyer_media_ad_type,
    first_value(appsflyer_affiliate_cost_model) over (
      partition by platform_users_natural_key, event_name 
      order by event_ts 
      rows between unbounded preceding and unbounded following
    ) as appsflyer_affiliate_cost_model,
    first_value(appsflyer_affiliate_cost_currency) over (
      partition by platform_users_natural_key, event_name 
      order by event_ts 
      rows between unbounded preceding and unbounded following
     ) as appsflyer_affiliate_cost_currency

  from events

)

select * from dedup
where platform_users_natural_key is not null
and event_name in ('install', 'regsuccess', 'ftd')