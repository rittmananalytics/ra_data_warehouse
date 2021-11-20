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
    {{ source('fivetran_mixpanel_events','events') }}
  ),
renamed_full AS (
  SELECT
    CAST(event_id AS string)      AS event_id,
    name AS event_type,
    replace(JSON_EXTRACT(properties, '$.path'),'"','') AS event_property_path,
    replace(JSON_EXTRACT(properties, '$.title'),'"','') AS event_property_title,
    replace(JSON_EXTRACT(properties, '$.url'),'"','') AS event_property_url,
    replace(JSON_EXTRACT(properties, '$.target'),'"','') AS event_property_target,
    replace(JSON_EXTRACT(properties, '$.episode'),'"','') AS event_property_episode,
    replace(JSON_EXTRACT(properties, '$.product'),'"','') AS event_property_product,
    replace(JSON_EXTRACT(properties, '$.type'),'"','') AS event_property_type,
    time AS event_ts,
    current_url AS event_current_url,
    mp_processing_time_ms AS event_processing_ts,
    insert_id AS event_insert_id,
    distinct_id AS user_id,
    browser AS browser_type,
    browser_version  AS browser_version,
    city AS city,
    device AS device,
    device_id AS device_id,
    mp_country_code AS country_code,
    os AS os,
    region AS user_region,
    screen_height AS screen_height,
    screen_width AS screen_width,
    search_engine AS search_engine,
    initial_referrer AS initial_referrer,
    initial_referring_domain AS initial_referring_domain,
    referring_domain AS referring_domain,
    referrer AS referrer
FROM
  source
)
,
renamed AS (
    SELECT
    event_id      AS event_id,
    event_type                  AS event_type,
    event_ts                   AS event_ts,
    coalesce(event_property_episode,event_property_product,event_property_target,CONCAT(event_property_type,event_property_product)) AS event_details,
    event_property_title        AS page_title,
    event_property_path         AS page_url_path,
    replace(
        {{ dbt_utils.get_url_host('referrer') }},
        'www.',
        ''
    )                           AS referrer_host,
    search_engine               AS search,
    event_current_url           AS page_url,
    {{ dbt_utils.get_url_host('event_current_url') }} AS page_url_host,
    {{ dbt_utils.get_url_parameter('event_current_url', 'gclid') }} AS gclid,
    CAST(null AS {{ dbt_utils.type_string() }})        AS utm_term,
    CAST(null AS {{ dbt_utils.type_string() }})        AS utm_content,
    CAST(null AS {{ dbt_utils.type_string() }})        AS utm_medium,
    CAST(null AS {{ dbt_utils.type_string() }})        AS utm_campaign,
    CAST(null AS {{ dbt_utils.type_string() }})        AS utm_source,
    CAST(null AS {{ dbt_utils.type_string() }})        AS ip,
    user_id                     AS visitor_id,
    user_id                     AS user_id,
    device                      AS device,
    '{{ var('stg_mixpanel_events_site') }}'  AS site
    FROM renamed_full
    where event_type != 'Loaded a Page'
    ),
    final AS (

        SELECT
            *,
            case
                when device = 'iPhone' then 'iPhone'
                when device = 'Android' then 'Android'
                when device in ('iPad', 'iPod') then 'Tablet'
                when device in ('Windows', 'Macintosh', 'X11') then 'Desktop'
                else 'Uncategorized'
            end AS device_category
        FROM renamed

    )
SELECT * FROM final

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
