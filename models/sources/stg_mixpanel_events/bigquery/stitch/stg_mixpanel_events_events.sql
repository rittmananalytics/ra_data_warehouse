{{config
  (enabled =
      (target.type == 'bigquery' and var("stg_mixpanel_events_etl") == 'stitch')
   )
}}
{% if var("product_warehouse_event_sources") %}
{% if 'mixpanel_events' in var("product_warehouse_event_sources") %}

WITH source as (
  {{ filter_stitch_relation(relation=var('stg_mixpanel_events_stitch_event_table'),unique_column='mp_reserved_insert_id') }}
),
renamed_full as (
  SELECT
    cast(mp_reserved_insert_id as string)      as event_id,
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
,
renamed as (
    select
    event_id      as event_id,
    event_type                  as event_type,
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
    where event_type != 'Loaded a Page'
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
