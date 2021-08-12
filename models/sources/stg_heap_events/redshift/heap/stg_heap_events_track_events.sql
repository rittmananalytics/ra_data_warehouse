{{config(enabled = target.type == 'redshift')}}
{% if var("product_warehouse_event_sources") %}
{% if 'heap_events_track' in var("product_warehouse_event_sources") %}

{{
    config(
        materialized="table"
    )
}}
with
recursive migrated_users(from_user_id, to_user_id, level) as
( select from_user_id, to_user_id, 1 as level
from {{ var('stg_heap_events_heap_user_migrations_table') }}
union all
select u.from_user_id, u.to_user_id, level + 1
from {{ var('stg_heap_events_heap_user_migrations_table') }} u, migrated_users m
where u.to_user_id = m.from_user_id and level < 4
),
mapped_user_ids as (
select from_user_id, to_user_id from migrated_users order by to_user_id)
,source as (

    select * from {{ var('stg_heap_events_heap_tracks_table') }}
    where time > CURRENT_DATE - interval '2 year'


),
   users as (

  select * from {{ var('stg_heap_events_heap_users_table') }}
   )
,
   sessions as (

  select * from {{ var('stg_heap_events_heap_sessions_table') }}
  where time > CURRENT_DATE - interval '2 year'
   )
,
renamed as (

  SELECT
    a.event_id::varchar AS event_id,
    event_table_name AS event_type,
    a.time AS event_ts,
    cast(null as varchar) AS event_details,
    cast(NULL AS varchar) AS page_title,
    cast(NULL AS varchar) AS page_url_path,
    replace(
        {{ dbt_utils.get_url_host('referrer') }},
        'www.',
       ''
    )                           as referrer_host,
    cast(NULL AS varchar) AS search,
    cast(NULL AS varchar) AS page_url,
    {{ dbt_utils.get_url_host('landing_page') }} as page_url_host,
    cast(NULL AS varchar) AS gclid,
    s.utm_term AS utm_term,
    s.utm_content AS utm_content,
    s.utm_medium AS utm_medium,
    s.utm_campaign AS utm_campaign,
    s.utm_source AS utm_source,
    s.ip AS ip,
    a.user_id::varchar AS visitor_id,
    u."identity" AS user_id,
    cast(null as varchar) AS device,
    device as device_category,
    'intostudy.com' AS site
FROM
    source a
JOIN sessions s ON a.session_id = s.session_id
LEFT JOIN mapped_user_ids m on a.user_id = m.from_user_id
JOIN users u ON coalesce(m.to_user_id,a.user_id) = u.user_id
WHERE a.event_table_name not ilike 'pageviews%'

)
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
