{{config(enabled = target.type == 'redshift')}}
{% if var("product_warehouse_event_sources") %}
{% if 'heap_events_page' in var("product_warehouse_event_sources") %}
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
 ,
 source as (

   select * from {{ var('stg_heap_events_heap_pages_table') }}
   where time > CURRENT_DATE - interval '2 year'


 ),
    users as (

   select * from {{ var('stg_heap_events_heap_users_table') }}
    )
 ,
renamed as (

  SELECT
    event_id::varchar AS event_id,
    'Page View' AS event_type,
    time AS event_ts,
    title AS event_details,
    title AS page_title,
    path AS page_url_path,
    replace(
        {{ dbt_utils.get_url_host('referrer') }},
        'www.',
        ''
    )                           as referrer_host,
    query AS search,
    concat(DOMAIN, path) AS page_url,
    domain as page_url_host,
    cast(NULL AS varchar) AS gclid,
    utm_term AS utm_term,
    utm_content AS utm_content,
    utm_medium AS utm_medium,
    utm_campaign AS utm_campaign,
    utm_source AS utm_source,
    ip AS ip,
    p.user_id::varchar AS visitor_id,
    u."identity" AS user_id,
    platform AS device,
    device as device_category,
    DOMAIN AS site
FROM
    source p
LEFT JOIN mapped_user_ids m on p.user_id = m.from_user_id
JOIN users u ON coalesce(m.to_user_id,p.user_id) = u.user_id

)
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
