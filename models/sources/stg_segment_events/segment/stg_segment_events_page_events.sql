{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("product_warehouse_event_sources") %}
{% if 'segment_events_page' in var("product_warehouse_event_sources") %}

with source AS (

  SELECT * FROM {{ source('segment', 'pages') }}

),

renamed AS (

    SELECT
        id                                                                   AS event_id,
        'Page View'                                                          AS event_type,
        received_at                                                          AS event_ts,
        CAST(context_page_title AS {{ dbt_utils.type_string() }})            AS event_details,
        CAST(context_page_title AS {{ dbt_utils.type_string() }})            AS page_title,
        CAST(path AS {{ dbt_utils.type_string() }})                          AS page_url_path,
        CAST(replace(
            {{ dbt_utils.get_url_host('context_page_referrer') }},
            'www.',
            ''
        ) AS {{ dbt_utils.type_string() }})                                 AS referrer_host,
        CAST(search AS {{ dbt_utils.type_string() }})                       AS search,
        CAST(url AS {{ dbt_utils.type_string() }})                          AS page_url,
        CAST({{ dbt_utils.get_url_host('url') }} AS {{ dbt_utils.type_string() }})  AS page_url_host,
        CAST({{ dbt_utils.get_url_parameter('url', 'gclid') }} AS {{ dbt_utils.type_string() }})  AS gclid,
        CAST(context_campaign_term AS {{ dbt_utils.type_string() }})        AS utm_term,
        CAST(context_campaign_content AS {{ dbt_utils.type_string() }})     AS utm_content,
        CAST(context_campaign_medium AS {{ dbt_utils.type_string() }})      AS utm_medium,
        CAST(context_campaign_name AS {{ dbt_utils.type_string() }})        AS utm_campaign,
        CAST(context_campaign_source AS {{ dbt_utils.type_string() }})      AS utm_source,
        CAST(context_ip AS {{ dbt_utils.type_string() }})                   AS ip,
        CAST(anonymous_id AS {{ dbt_utils.type_string() }})                 AS visitor_id,
        CAST(user_id AS {{ dbt_utils.type_string() }})                      AS user_id,
        case
            when lower(context_user_agent) like '%android%' then 'Android'
            else replace(
              {{ dbt_utils.split_part("context_user_agent","'('","1") }},
                ';', '')
        end  AS device,
        CAST('{{ var('stg_segment_events_site') }}' AS {{ dbt_utils.type_string() }})  AS site


    FROM source

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
{% else %} {{config(enabled=false)}} {% endif %}
