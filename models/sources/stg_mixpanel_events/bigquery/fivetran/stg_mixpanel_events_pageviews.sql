{{config
  (enabled =
      (target.type == 'bigquery' and var("stg_mixpanel_events_etl") == 'fivetran')
   )
}}
{% if var("product_warehouse_event_sources") %}
{% if 'mixpanel_events' in var("product_warehouse_event_sources") %}


  WITH source AS (

    SELECT
      *
    FROM
    {{ var('stg_mixpanel_events_fivetran_event_table') }}
  ),
renamed_full as (
  SELECT
  cast(event_id as string)       as event_id,
  name as event_type,
  replace(JSON_EXTRACT(properties, '$.path'),'"','') as event_property_path,
  replace(JSON_EXTRACT(properties, '$.title'),'"','') as event_property_title,
  replace(JSON_EXTRACT(properties, '$.url'),'"','') as event_property_url,
  replace(JSON_EXTRACT(properties, '$.target'),'"','') as event_property_target,
  replace(JSON_EXTRACT(properties, '$.episode'),'"','') as event_property_episode,
  replace(JSON_EXTRACT(properties, '$.product'),'"','') as event_property_product,
  replace(JSON_EXTRACT(properties, '$.type'),'"','') as event_property_type,
  time as event_ts,
  current_url as event_current_url,
  mp_processing_time_ms as event_processing_ts,
  insert_id as event_insert_id,
  distinct_id as user_id,
  browser as browser_type,
  browser_version  as browser_version,
  city as city,
  device as device,
  device_id as device_id,
  mp_country_code as country_code,
  os as os,
  region as user_region,
  screen_height as screen_height,
  screen_width as screen_width,
  search_engine as search_engine,
  initial_referrer as initial_referrer,
  initial_referring_domain as initial_referring_domain,
  referring_domain as referring_domain,
  referrer as referrer
FROM
  source
)
,
renamed as (
select
event_id                    as event_id,
'Page View'                  as event_type,
event_ts                   as event_ts,
coalesce(event_property_episode,event_property_product,event_property_target,concat(event_property_type,event_property_product)) as event_details,
event_property_title        as page_title,
event_property_path         as page_url_path,
replace(
    {{ dbt_utils.get_url_host('referrer') }},
    'www.',
    ''
)                           as referrer_host,
search_engine               as search,
event_current_url           as page_url,
{{ dbt_utils.get_url_host('event_current_url') }} as page_url_host,
{{ dbt_utils.get_url_parameter('event_current_url', 'gclid') }} as gclid,
cast(null as string)        as utm_term,
cast(null as string)        as utm_content,
cast(null as string)        as utm_medium,
cast(null as string)        as utm_campaign,
cast(null as string)        as utm_source,
cast(null as string)        as ip,
user_id                     as visitor_id,
user_id                     as user_id,
device                      as device,
'{{ var('stg_mixpanel_events_site') }}'  as site

from renamed_full
where event_type = 'Loaded a Page'
),
final as (

    select
        *,
        case
            when device = 'iPhone' then 'iPhone'
            when device = 'Android' then 'Android'
            when device in ('iPad', 'iPod') then 'Tablet'
            when device in ('Windows', 'Macintosh', 'X11') then 'Desktop'
            else 'Uncategorized'
        end as device_category
    from renamed

)
select * from final

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
