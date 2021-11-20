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
    {{ source('custom_source_2','s_projects' ) }}
),
renamed AS (
SELECT
       CONCAT('custom_2-',id)                                       AS timesheet_project_id,
       CAST(null AS {{ dbt_utils.type_string() }})                     AS company_id,
       CAST(null AS {{ dbt_utils.type_string() }})                     AS project_name,
       CAST(null AS {{ dbt_utils.type_string() }})                     AS project_code,
        CAST(null AS {{ dbt_utils.type_timestamp() }})                  AS project_delivery_start_ts,
        CAST(null AS {{ dbt_utils.type_timestamp() }})                  AS project_delivery_end_ts,
       CAST(null AS {{ dbt_utils.type_boolean() }})                    AS project_is_active,
       CAST(null AS {{ dbt_utils.type_boolean() }})                    AS project_is_billable,
       CAST(null AS numeric)                    AS project_hourly_rate,
       CAST(null AS numeric)                    AS project_cost_budget,
       CAST(null AS {{ dbt_utils.type_boolean() }})                    AS project_is_fixed_fee,
       CAST(null AS {{ dbt_utils.type_boolean() }})s                   AS project_is_expenses_included_in_cost_budget,
       CAST(null AS numeric)                    AS project_fee_amount,
       CAST(null AS numeric)                    AS project_budget_amount,
       CAST(null AS numeric)                    AS project_over_budget_notification_pct,
       CAST(null AS {{ dbt_utils.type_string() }})                     AS project_budget_by
FROM source p)
SELECT
  *
FROM
  renamed
