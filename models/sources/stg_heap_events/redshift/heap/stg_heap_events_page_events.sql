{{ config(
  enabled = target.type == 'redshift'
) }}

{% if var("product_warehouse_event_sources") %}
  {% if 'heap_events_page' in var("product_warehouse_event_sources") %}
    {{ config(
      materialized = "table"
    ) }}

    WITH recursive migrated_users(
      from_user_id,
      to_user_id,
      LEVEL
    ) AS (

      SELECT
        from_user_id,
        to_user_id,
        1 AS LEVEL
      FROM
        {{ source(
          'heap',
          'user_migrations'
        ) }}
      UNION ALL
      SELECT
        u.from_user_id,
        u.to_user_id,
        LEVEL + 1
      FROM
        {{ source(
          'heap',
          'user_migrations'
        ) }}
        u,
        migrated_users m
      WHERE
        u.to_user_id = m.from_user_id
        AND LEVEL < 4
    ),
    mapped_user_ids AS (
      SELECT
        from_user_id,
        to_user_id
      FROM
        migrated_users
      ORDER BY
        to_user_id
    ),
    source AS (
      SELECT
        *
      FROM
        {{ source(
          'heap',
          'pages'
        ) }}
      WHERE
        TIME > CURRENT_DATE - INTERVAL '2 year'
    ),
    users AS (
      SELECT
        *
      FROM
        {{ source(
          'heap',
          'users'
        ) }}
    ),
    renamed AS (
      SELECT
        CAST(event_id AS {{ dbt_utils.type_string() }}) AS event_id,
        'Page View' AS event_type,
        TIME AS event_ts,
        title AS event_details,
        title AS page_title,
        path AS page_url_path,
        REPLACE({{ dbt_utils.get_url_host('referrer') }}, 'www.', '') AS referrer_host,
        query AS search,
        CONCAT(
          domain,
          path
        ) AS page_url,
        domain AS page_url_host,
        CAST(NULL AS {{ dbt_utils.type_string() }}) AS gclid,
        utm_term AS utm_term,
        utm_content AS utm_content,
        utm_medium AS utm_medium,
        utm_campaign AS utm_campaign,
        utm_source AS utm_source,
        ip AS ip,
        CAST(p.user_id AS {{ dbt_utils.type_string() }}) AS visitor_id,
        u."identity" AS user_id,
        platform AS device,
        device AS device_category,
        domain AS site,
        ,
        'Snowplow (Try Snowplow Trial)'                                     AS source,
        platform                                                            AS platform,
        CAST(null as {{ dbt_utils.type_string() }})                         AS ip_country,
        CAST(null as {{ dbt_utils.type_string() }})                         AS ip_region,
        CAST(city as {{ dbt_utils.type_string() }}) 	                      AS ip_city,
        CAST(postal_code as {{ dbt_utils.type_string() }}) 	                      AS ip_zipcode,
        CAST(null as {{ dbt_utils.type_float() }})                          AS ip_latitude,
        CAST(null as {{ dbt_utils.type_float() }}) 	                        AS ip_longitude,
        CAST(null as {{ dbt_utils.type_string() }}) 	                      AS ip_region_name,
        CAST(null as {{ dbt_utils.type_string() }}) 	                      AS ip_isp,
        CAST(null as {{ dbt_utils.type_string() }}) 	                      AS ip_organization,
        CAST(null as {{ dbt_utils.type_string() }})                         AS ip_domain
      FROM
        source p
        LEFT JOIN mapped_user_ids m
        ON p.user_id = m.from_user_id
        JOIN users u
        ON COALESCE(
          m.to_user_id,
          p.user_id
        ) = u.user_id
    )
  SELECT
    *
  FROM
    renamed
  {% else %}
    {{ config(
      enabled = false
    ) }}
  {% endif %}
{% else %}
  {{ config(
    enabled = false
  ) }}
{% endif %}
