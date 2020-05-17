{% if not var("enable_mixpanel_events_source") %}
  {{ config(
    enabled = false
  ) }}
{% endif %}

{% if var("mixpanel_events_source_type") == 'fivetran' %}
  WITH source AS (

    SELECT
      *
    FROM
    {{ target.database}}.{{ var('fivetran_event_table') }}
  ),
renamed as (
  SELECT
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
{% elif var("mixpanel_events_source_type") == 'stitch' %}
WITH source as (
  {{ filter_stitch_table(var('stitch_export_table'),'mp_reserved_insert_id') }}

),
renamed as (
  SELECT
     event as event_type,
     path as event_property_path,
     title as event_property_title,
     url as event_property_url,
     target as event_property_target,
     episode as event_property_episode,
     product as event_property_product,
     type as event_property_type,
     time as event_ts,
     mp_reserved_current_url as event_current_url,
     mp_processing_time_ms as event_processing_ts,
     mp_reserved_insert_id as event_insert_id,
     distinct_id as user_id,
     mp_reserved_browser as browser_type,
     mp_reserved_browser_version  as browser_version,
     mp_reserved_city as city,
     mp_reserved_device as device,
     mp_reserved_device_id as device_id,
     mp_country_code as country_code,
     mp_reserved_os as os,
     mp_reserved_region as region,
     mp_reserved_screen_height as screen_height,
     mp_reserved_screen_width as screen_width,
     mp_reserved_search_engine as search_engine,
     mp_reserved_initial_referrer as initial_referrer,
     mp_reserved_initial_referring_domain as referring_domain,
     mp_reserved_referring_domain as referring_domain,
     referrer as referrer
FROM
  source
)
{% endif %}
select * from renamed
