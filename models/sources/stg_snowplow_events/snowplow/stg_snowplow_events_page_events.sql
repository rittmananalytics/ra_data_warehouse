{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("product_warehouse_event_sources") %}
{% if 'snowplow_events_page' in var("product_warehouse_event_sources") %}

with source AS (

  SELECT * FROM {{ source('snowplow', 'pages') }}

),

renamed AS (

  SELECT
    CAST(event_id AS {{ dbt_utils.type_string() }})           AS event_id,
    CAST(event AS {{ dbt_utils.type_string() }})              AS event_type,
    dvce_created_tstamp                                       AS event_ts,
    CAST(page_title AS {{ dbt_utils.type_string() }})         AS event_details,
    CAST(page_title AS {{ dbt_utils.type_string() }})         AS page_title,
    CAST(page_urlpath AS {{ dbt_utils.type_string() }})       AS page_url_path,
    CAST(refr_urlhost AS {{ dbt_utils.type_string() }})       AS referrer_host,
    CAST(null AS {{ dbt_utils.type_string() }})               AS search,
    CAST(page_url AS {{ dbt_utils.type_string() }})           AS page_url,
    CAST(page_urlhost AS {{ dbt_utils.type_string() }})       AS page_url_host,
    CAST(null AS {{ dbt_utils.type_string() }})               AS gclid,
    CAST(mkt_term AS {{ dbt_utils.type_string() }})           AS utm_term,
    CAST(mkt_content AS {{ dbt_utils.type_string() }})        AS utm_content,
    CAST(mkt_medium AS {{ dbt_utils.type_string() }})         AS utm_medium,
    CAST(mkt_campaign AS {{ dbt_utils.type_string() }})       AS utm_campaign,
    CAST(mkt_source AS {{ dbt_utils.type_string() }})         AS utm_source,
    CAST(user_ipaddress AS {{ dbt_utils.type_string() }})     AS ip,
    CAST(coalesce(network_userid,domain_userid) AS {{ dbt_utils.type_string() }}) AS visitor_id,
    CAST(user_id AS {{ dbt_utils.type_string() }})            AS user_id,
    case
        when lower(useragent) like '%android%' then 'Android'
        else replace(
          {{ dbt_utils.split_part("useragent","'('","1") }},
            ';', '')
    end  AS device,
    CAST(page_urlhost AS {{ dbt_utils.type_string() }})       AS site
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
