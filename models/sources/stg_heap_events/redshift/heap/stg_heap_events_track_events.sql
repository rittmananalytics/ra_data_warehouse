{{ config(
  enabled = target.type == 'redshift'
) }}

{% if var("product_warehouse_event_sources") %}
  {% if 'heap_events_track' in var("product_warehouse_event_sources") %}
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
          'tracks'
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
    sessions AS (
      SELECT
        *
      FROM
        {{ source(
          'heap',
          'sessions'
        ) }}
      WHERE
        TIME > CURRENT_DATE - INTERVAL '2 year'
    ),
    renamed AS (
      SELECT
        CAST(A.event_id AS {{ dbt_utils.type_string() }}) AS event_id,
        event_table_name AS event_type,
        A.time AS event_ts,
        CAST(NULL AS {{ dbt_utils.type_string() }}) AS event_details,
        CAST(NULL AS {{ dbt_utils.type_string() }}) AS page_title,
        CAST(NULL AS {{ dbt_utils.type_string() }}) AS page_url_path,
        REPLACE({{ dbt_utils.get_url_host('referrer') }}, 'www.', '') AS referrer_host,
        CAST(NULL AS {{ dbt_utils.type_string() }}) AS search,
        CAST(NULL AS {{ dbt_utils.type_string() }}) AS page_url,
        {{ dbt_utils.get_url_host('landing_page') }} AS page_url_host,
        CAST(NULL AS {{ dbt_utils.type_string() }}) AS gclid,
        s.utm_term AS utm_term,
        s.utm_content AS utm_content,
        s.utm_medium AS utm_medium,
        s.utm_campaign AS utm_campaign,
        s.utm_source AS utm_source,
        s.ip AS ip,
        CAST(A.user_id AS {{ dbt_utils.type_string() }}) AS visitor_id,
        u."identity" AS user_id,
        CAST(NULL AS {{ dbt_utils.type_string() }}) AS device,
        device AS device_category,
        {{ var('stg_heap_events_site') }} AS site
      FROM
        source A
        JOIN sessions s
        ON A.session_id = s.session_id
        LEFT JOIN mapped_user_ids m
        ON A.user_id = m.from_user_id
        JOIN users u
        ON COALESCE(
          m.to_user_id,
          A.user_id
        ) = u.user_id
      WHERE
        A.event_table_name NOT ILIKE 'pageviews%'
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
