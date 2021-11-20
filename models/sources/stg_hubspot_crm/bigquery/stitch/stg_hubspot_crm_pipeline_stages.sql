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
      pipelineid AS pipeline_id,
      stages.value.stageid AS pipeline_stage_id,
      stages.value.label AS pipeline_stage_label,
      stages.value.displayorder AS pipeline_stage_display_order,
      stages.value.probability AS pipeline_stage_close_probability_pct,
      stages.value.closedwon AS pipeline_stage_closed_won
    FROM source,
    unnest (stages) stages
)
SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
