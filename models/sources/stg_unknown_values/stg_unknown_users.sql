{% if var("projects_warehouse_delivery_sources") %}

SELECT
  'Unassigned' AS user_id,
  'Unassigned' AS user_name,
  'Unassigned' AS user_email,
  FALSE AS contact_is_contractor,
  FALSE AS contact_is_staff,
  0 AS contact_weekly_capacity,
  CAST(
    NULL AS STRING
  ) AS user_phone,
  0 AS contact_default_hourly_rate,
  0 AS contact_cost_rate,
  FALSE AS contact_is_active,
  CAST(
    NULL AS TIMESTAMP
  ) AS user_created_ts,
  CAST(
    NULL AS TIMESTAMP
  ) AS user_last_modified_ts

  {% else %} {{config(enabled=false)}} {% endif %}
