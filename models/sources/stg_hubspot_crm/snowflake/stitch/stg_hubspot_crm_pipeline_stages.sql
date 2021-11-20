{{config(enabled = target.type == 'snowflake')}}
{% if var("marketing_warehouse_deal_sources") %}
{% if 'hubspot_crm' in var("marketing_warehouse_deal_sources") %}

{% if var("stg_hubspot_crm_etl") == 'fivetran' %}
with source AS (
  SELECT * from
  FROM {{ var('stg_hubspot_crm_fivetran_pipeline_stages_table') }}

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
{% elif var("stg_hubspot_crm_etl") == 'stitch' %}
with source AS (
  {{ filter_stitch_relation(relation=var('stg_hubspot_crm_stitch_deal_pipelines_table'),unique_column='pipelineid') }}

),
renamed AS (
    SELECT
      pipelineid AS pipeline_id,
      stages:stageid::STRING AS pipeline_stage_id,
      stages:label::STRING AS pipeline_stage_label,
      stages:displayorder::INT AS pipeline_stage_display_order,
      stages:probability::FLOAT pipeline_stage_close_probability_pct,
      stages:closedwon::BOOLEAN AS pipeline_stage_closed_won
    FROM source
)
{% endif %}
SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
