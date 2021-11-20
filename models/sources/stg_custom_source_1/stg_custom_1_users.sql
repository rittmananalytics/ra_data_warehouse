{% if not var("enable_custom_source_1") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
  {{ filter_stitch_table(var('custom_source_1'),var('stitch_projects_table'),'gid') }}

),
renamed AS (
SELECT
       CONCAT('custom_1-',id)                     AS user_id,
       CAST(null AS {{ dbt_utils.type_string() }})                     AS user_name,
       CAST(null AS {{ dbt_utils.type_string() }})                     AS user_email,
       CAST(null AS {{ dbt_utils.type_boolean() }})                    AS contact_is_contractor,
       CAST(null AS {{ dbt_utils.type_boolean() }})                    AS contact_is_staff,
       CAST(null AS numeric)                    AS contact_weekly_capacity,
       CAST(null AS {{ dbt_utils.type_string() }})                     AS user_phone,
       CAST(null AS numeric)                    AS contact_default_hourly_rate,
       CAST(null AS numeric)                    AS contact_cost_rate,
       CAST(null AS {{ dbt_utils.type_boolean() }})                    AS contact_is_active,
        CAST(null AS {{ dbt_utils.type_timestamp() }})                  AS user_created_ts,
        CAST(null AS {{ dbt_utils.type_timestamp() }})                  AS user_last_modified_ts
FROM source)
SELECT
  *
FROM
  renamed
