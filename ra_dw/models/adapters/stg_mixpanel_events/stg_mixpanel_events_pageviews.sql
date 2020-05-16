{% if not var("enable_mixpanel_events_source") %}
  {{ config(
    enabled = false
  ) }}
{% endif %}
with renamed as (
select
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
device                      as device
from {{ ref('stg_mixpanel_events_all_events')}}
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
