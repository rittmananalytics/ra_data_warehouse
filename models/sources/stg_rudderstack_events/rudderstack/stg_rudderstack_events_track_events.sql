{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("product_warehouse_event_sources") %}
{% if 'rudderstack_events_track' in var("product_warehouse_event_sources") %}

{{
    config(
        materialized="table"
    )
}}
with source AS (

    SELECT * FROM {{ source('rudderstack', 'tracks') }}

),

renamed AS (

    SELECT
        id                          AS event_id,
        event                       AS event_type,
        received_at                 AS event_ts,
        event_text                  AS event_details,
        CAST(null AS string )       AS page_title,
        context_page_path           AS page_url_path,
        replace(
            {{ dbt_utils.get_url_host('context_page_referrer') }},
            'www.',
            ''
        )                           AS referrer_host,
        context_page_search         AS search,
        context_page_url            AS page_url,
        {{ dbt_utils.get_url_host('context_page_url') }} AS page_url_host,
        {{ dbt_utils.get_url_parameter('context_page_url', 'gclid') }} AS gclid,
        CAST(null AS {{ dbt_utils.type_string() }})        AS utm_term,
        CAST(null AS {{ dbt_utils.type_string() }})     AS utm_content,
        CAST(null AS {{ dbt_utils.type_string() }})      AS utm_medium,
        CAST(null AS {{ dbt_utils.type_string() }})        AS utm_campaign,
        CAST(null AS {{ dbt_utils.type_string() }})      AS utm_source,
        context_ip                  AS ip,
        anonymous_id                AS visitor_id,
        user_id                     AS user_id,
        case
            when lower(context_user_agent) like '%android%' then 'Android'
            else replace(
              {{ dbt_utils.split_part("context_user_agent","'('","1") }},
                ';', '')
        end  AS device,
        '{{ var('stg_segment_events_site') }}'  AS site,
        CAST(null as {{ dbt_utils.type_string() }})                                   AS session_seq,
        CAST(null as {{ dbt_utils.type_string() }})                                   AS session_id,
        'Snowplow (Try Snowplow Trial)'                                     AS source,
        'web'                                                               AS platform,
        CAST(null as {{ dbt_utils.type_string() }})                         AS ip_country,
        CAST(null as {{ dbt_utils.type_string() }})                         AS ip_region,
        CAST(null as {{ dbt_utils.type_string() }}) 	                      AS ip_city,
        CAST(null as {{ dbt_utils.type_string() }}) 	                      AS ip_zipcode,
        CAST(null as {{ dbt_utils.type_float() }})                          AS ip_latitude,
        CAST(null as {{ dbt_utils.type_float() }}) 	                        AS ip_longitude,
        CAST(null as {{ dbt_utils.type_string() }}) 	                      AS ip_region_name,
        CAST(null as {{ dbt_utils.type_string() }}) 	                      AS ip_isp,
        CAST(null as {{ dbt_utils.type_string() }}) 	                      AS ip_organization,
        CAST(null as {{ dbt_utils.type_string() }})                         AS ip_domain
    FROM source

)
,
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
{% else %} {{config(enabled=false)}} {% endif %}
