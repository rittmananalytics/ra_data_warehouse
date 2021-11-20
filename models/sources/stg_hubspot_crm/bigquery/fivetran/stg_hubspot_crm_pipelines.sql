{{config(enabled = target.type == 'bigquery')}}
{% if var("marketing_warehouse_deal_sources") %}
{% if 'hubspot_crm' in var("marketing_warehouse_deal_sources") %}

with source AS (
  SELECT * from
  FROM {{ source('fivetran_hubspot_crm','pipelines') }}
),
renamed AS (
    SELECT
      label AS pipeline_label,
      pipeline_id,
      display_order AS pipeline_display_order,
      active AS pipeline_active
    FROM source
)
SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
