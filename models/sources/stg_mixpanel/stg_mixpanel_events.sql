with source as (

  select * from {{ source('mixpanel', 's_events') }}
  where time >= '2020-05-01'

),

user_ref as (

  select distinct
    cast(user_id as string) as platform_users_natural_key,
    cast(distinct_id as string) as mixpanel_anonymous_natural_key
  
  from source

  where user_id is not null

), 

events as (

  select distinct
    cast(event_id as string) as mixpanel_events_natural_key,
    cast(user_id as string) as platform_users_natural_key,
    cast(distinct_id as string) as mixpanel_anonymous_natural_key, 

    cast(time as timestamp) as event_ts,

    lower(initial_referrer) as initial_referrer, 
    lower(initial_referring_domain) as initial_referring_domain, 
    lower(referrer) as referrer, 
    lower(referring_domain) as referring_domain, 
    lower(search_engine) as search_engine, 
    lower(name) as event_name,
    lower(properties) as event_properties

  from source

  where event_id is not null

), 

add_missing_user_ids as (

  select
    events.mixpanel_events_natural_key,
    coalesce(events.platform_users_natural_key, user_ref.platform_users_natural_key) as platform_users_natural_key, 
    events.mixpanel_anonymous_natural_key, 

    events.event_ts,

    events.initial_referrer, 
    events.initial_referring_domain, 
    events.referrer, 
    events.referring_domain, 
    events.search_engine, 
    events.event_name,
    events.event_properties
  
  from events
  left join user_ref using (mixpanel_anonymous_natural_key)

), 

dedup as (

  select distinct
    mixpanel_events_natural_key,
    max(platform_users_natural_key) over (partition by mixpanel_events_natural_key) as platform_users_natural_key, 
    mixpanel_anonymous_natural_key, 

    event_ts,

    initial_referrer, 
    initial_referring_domain, 
    referrer, 
    referring_domain, 
    search_engine, 
    event_name,
    event_properties
  
  from add_missing_user_ids

)

select * from dedup
where event_name in (
  'accdetails', 
  'address', 
  'authenticate', 
  'biometrics', 
  'bioskip', 
  'contpref', 
  'country', 
  'depositmade', 
  'dob', 
  'ekyc', 
  'ekycfail', 
  'install', 
  'joinnow', 
  'faceid', 
  'faceskip', 
  'ftd', 
  'kycdocupload', 
  'kycdocsub', 
  'login',
  'manualaddress',  
  'onboarding', 
  'perdetails', 
  'pinskip', 
  'pre reg 1', 
  'pre reg 2', 
  'profiletab', 
  'recact', 
  'refcode', 
  'refskip', 
  'regsuccess', 
  'setpin', 
  'smtbanner',
  'smtbclose',  
  'smtbview', 
  'touchid', 
  'touchskip', 
  'watchlistadd', 
  'watchlistremove', 
  'welcome', 
  'withdraw'
)