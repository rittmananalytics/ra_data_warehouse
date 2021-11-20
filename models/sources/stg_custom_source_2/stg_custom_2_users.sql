{% if not var("enable_custom_source_2") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
    SELECT *
    from
    {{ source('custom_source_2','s_transactions' ) }}
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
)
SELECT * FROM renamed
