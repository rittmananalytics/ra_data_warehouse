{{config
  (enabled =
      (target.type == 'bigquery' and var("stg_hubspot_crm_etl") == 'stitch')
   )
}}
{% if var("marketing_warehouse_deal_sources") %}
{% if 'hubspot_crm' in var("marketing_warehouse_deal_sources") %}

with source AS (
  {{ filter_stitch_relation(relation=source('stitch_hubspot_crm','deal_pipelines'),unique_column='pipelineid') }}
),
renamed AS (
    SELECT
      label AS pipeline_label,
      pipelineid AS pipeline_id,
      displayorder AS pipeline_display_order,
      active AS pipeline_active
    FROM source
)
SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
