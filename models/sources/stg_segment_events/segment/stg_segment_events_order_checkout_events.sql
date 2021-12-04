{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("product_warehouse_event_sources") %}
{% if 'segment_events_order_checkout' in var("product_warehouse_event_sources") %}


with source AS (

    SELECT * FROM {{ source('segment', 'order_checkout') }}

),

renamed AS (

    SELECT
        id                                                  AS event_id,
        event                                               AS event_type,
        received_at                                         AS event_ts,
        CAST(order_id AS {{ dbt_utils.type_string() }})     AS event_details,
        CAST(null AS {{ dbt_utils.type_string() }})         AS page_title,
        CAST(context_page_path AS {{ dbt_utils.type_string() }})                                    AS page_url_path,
        CAST(replace(
            {{ dbt_utils.get_url_host('context_page_referrer') }},
            'www.',
            ''
        )  AS {{ dbt_utils.type_string() }})                AS referrer_host,
        CAST(null AS {{ dbt_utils.type_string() }})         AS search,
        CAST(context_page_url AS {{ dbt_utils.type_string() }})                                    AS page_url,
        CAST({{ dbt_utils.get_url_host('context_page_url') }} AS {{ dbt_utils.type_string() }})    AS page_url_host,
        CAST({{ dbt_utils.get_url_parameter('context_page_url', 'gclid') }} AS {{ dbt_utils.type_string() }}) AS gclid,
        CAST(null AS {{ dbt_utils.type_string() }})         AS utm_term,
        CAST(null AS {{ dbt_utils.type_string() }})         AS utm_content,
        CAST(null AS {{ dbt_utils.type_string() }})         AS utm_medium,
        CAST(null AS {{ dbt_utils.type_string() }})         AS utm_campaign,
        CAST(null AS {{ dbt_utils.type_string() }})         AS utm_source,
        CAST(context_ip AS {{ dbt_utils.type_string() }})                                          AS ip,
        CAST(anonymous_id AS {{ dbt_utils.type_string() }})                                        AS visitor_id,
        CAST(user_id AS {{ dbt_utils.type_string() }})                                             AS user_id,
        case
            when lower(context_user_agent) like '%android%' then 'Android'
            else replace(
              {{ dbt_utils.split_part('context_user_agent',"'('",1) }},
                ';', '')
        end  AS device,
        '{{ var('stg_segment_events_site') }}'  AS site,
        'Segment'                                                           AS source,
        'ecommerce'                                                         AS platform,
        CAST(null as {{ dbt_utils.type_string() }})                         AS ip_country,
        CAST(null as {{ dbt_utils.type_string() }})                         AS ip_region,
        CAST(city as {{ dbt_utils.type_string() }}) 	                      AS ip_city,
        CAST(postal_code as {{ dbt_utils.type_string() }}) 	                AS ip_zipcode,
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
