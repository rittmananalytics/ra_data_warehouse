{{config(enabled = target.type == 'bigquery')}}
{% if var("marketing_warehouse_deal_sources") %}
{% if 'hubspot_crm' in var("marketing_warehouse_deal_sources") %}

with source AS (
  SELECT * from
  FROM {{ source('fivetran_hubspot_crm','pipeline_stages') }}

),
renamed AS (
    SELECT
      pipeline_id,
      stage_id AS pipeline_stage_id,
      label AS pipeline_stage_label,
      display_order AS pipeline_stage_display_order,
      probability AS pipeline_stage_close_probability_pct,
      closed_won AS pipeline_stage_closed_won
    FROM source
)
SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
