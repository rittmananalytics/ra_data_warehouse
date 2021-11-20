{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("product_warehouse_event_sources") %}
{% if 'appsflyer_events_track' in var("product_warehouse_event_sources") %}

with source AS (

  SELECT * FROM {{ source('appsflyer', 'tracks') }}

),

renamed AS (

SELECT

    CAST(appsflyer_id AS {{ dbt_utils.type_string() }})           AS event_id,
    CAST(event_name AS {{ dbt_utils.type_string() }})             AS event_type,
    event_time                                                    AS event_ts,
    CAST( event_value AS {{ dbt_utils.type_string() }})           AS event_details,
    CAST(app_name AS {{ dbt_utils.type_string() }})               AS page_title,
    CAST(null AS {{ dbt_utils.type_string() }})                   AS page_url_path,
    CAST(http_referrer AS {{ dbt_utils.type_string() }})          AS referrer_host,
    CAST(null AS {{ dbt_utils.type_string() }})                   AS search,
    CAST(original_url AS {{ dbt_utils.type_string() }})           AS page_url,
    CAST(null AS {{ dbt_utils.type_string() }})                   AS page_url_host,
    CAST(null AS {{ dbt_utils.type_string() }})                   AS gclid,
    CAST(af_keywords AS {{ dbt_utils.type_string() }})            AS utm_term,
    CAST(af_adset AS {{ dbt_utils.type_string() }})               AS utm_content,
    CAST(af_ad_type AS {{ dbt_utils.type_string() }})             AS utm_medium,
    CAST(campaign AS {{ dbt_utils.type_string() }})               AS utm_campaign,
    CAST(media_source AS {{ dbt_utils.type_string() }})           AS utm_source,
    CAST(ip AS {{ dbt_utils.type_string() }})                     AS ip,
    CAST(idfa AS {{ dbt_utils.type_string() }})                   AS visitor_id,
    CAST(customer_user_id AS {{ dbt_utils.type_string() }})       AS user_id,
    case
        when lower(user_agent) like '%android%' then 'Android'
        else replace(
          {{ dbt_utils.split_part("user_agent","'('","1") }},
            ';', '')
    end                                                           AS device,
    CAST(af_siteid AS {{ dbt_utils.type_string() }})              AS site
      FROM
    source
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
