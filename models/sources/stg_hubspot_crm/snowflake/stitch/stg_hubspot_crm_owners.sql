{{config(enabled = target.type == 'snowflake')}}
{% if var("marketing_warehouse_deal_sources") %}
{% if 'hubspot_crm' in var("marketing_warehouse_deal_sources") %}

{% if var("stg_hubspot_crm_etl") == 'fivetran' %}
WITH source AS (
  SELECT * from
  FROM {{ var('stg_hubspot_crm_fivetran_owners_table') }}
),
renamed AS (
    SELECT
      CAST(owner_id AS int) AS owner_id,
      CONCAT(CONCAT(first_name,' '),last_name) AS owner_full_name,
      first_name,
      last_name,
      email AS owner_email
    FROM source
)
{% elif var("stg_hubspot_crm_etl") == 'stitch' %}
WITH source AS (
  {{ filter_stitch_relation(relation=var('stg_hubspot_crm_stitch_owners_table'),unique_column='ownerid') }}

),
renamed AS (
    SELECT
      ownerid::STRING AS owner_id,
      CONCAT(CONCAT(firstname,' '),lastname) AS owner_full_name,
      firstname AS owner_first_name,
      lastname AS owner_last_name,
      email AS owner_email
    FROM source
)
{% endif %}
SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
