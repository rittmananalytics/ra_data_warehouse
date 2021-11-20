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
      label AS pipeline_label,
      pipeline_id,
      display_order AS pipeline_display_order,
      active AS pipeline_active
    FROM source
)
{% elif var("stg_hubspot_crm_etl") == 'stitch' %}
with source AS (
  {{ filter_stitch_relation(relation=var('stg_hubspot_crm_stitch_deal_pipelines_table'),unique_column='pipelineid') }}
),
renamed AS (
    SELECT
      label AS pipeline_label,
      pipelineid AS pipeline_id,
      displayorder AS pipeline_display_order,
      active AS pipeline_active
    FROM source
)
{% endif %}
SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
