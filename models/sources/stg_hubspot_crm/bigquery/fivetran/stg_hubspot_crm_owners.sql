{{config(enabled = target.type == 'bigquery')}}
{% if var("marketing_warehouse_deal_sources") %}
{% if 'hubspot_crm' in var("marketing_warehouse_deal_sources") %}

WITH source AS (
  SELECT * from
  FROM {{ source('fivetran_hubspot_crm','owners') }}
),
renamed AS (
    SELECT
      safe_CAST(owner_id AS int64) AS owner_id,
      CONCAT(CONCAT(first_name,' '),last_name) AS owner_full_name,
      first_name,
      last_name,
      email AS owner_email
    FROM source
)
SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
