{{config
  (enabled =
      (target.type == 'bigquery' and var("stg_hubspot_crm_etl") == 'stitch')
   )
}}
{% if var("marketing_warehouse_deal_sources") %}
{% if 'hubspot_crm' in var("marketing_warehouse_deal_sources") %}


WITH source AS (
  {{ filter_stitch_relation(relation=source('stitch_hubspot_crm','owners'),unique_column='ownerid') }}

),
renamed AS (
    SELECT
      safe_CAST(ownerid AS int64) AS owner_id,
      CONCAT(CONCAT(firstname,' '),lastname) AS owner_full_name,
      firstname AS owner_first_name,
      lastname AS owner_last_name,
      email AS owner_email
    FROM source
)
SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
