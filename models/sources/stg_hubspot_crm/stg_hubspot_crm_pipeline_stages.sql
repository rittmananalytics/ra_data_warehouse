{% if var("crm_warehouse_deal_sources") %}
{% if 'hubspot_crm' in var("crm_warehouse_deal_sources") %}

{% if var("stg_hubspot_crm_etl") == 'fivetran' %}
with source as (
  select * from
  from {{ var('stg_hubspot_crm_fivetran_pipeline_stages_table') }}

),
renamed as (
    select
      pipeline_id,
      stage_id as pipeline_stage_id,
      label as pipeline_stage_label,
      display_order as pipeline_stage_display_order,
      probability as pipeline_stage_close_probability_pct,
      closed_won as pipeline_stage_closed_won
    from source
)
{% elif var("stg_hubspot_crm_etl") == 'stitch' %}
with source as (
  {{ filter_stitch_relation(relation=var('stg_hubspot_crm_stitch_deal_pipelines_table'),unique_column='pipelineid') }}

),
renamed as (
    select
      pipelineid as pipeline_id,
      stages.value.stageid as pipeline_stage_id,
      stages.value.label as pipeline_stage_label,
      stages.value.displayorder as pipeline_stage_display_order,
      stages.value.probability as pipeline_stage_close_probability_pct,
      stages.value.closedwon as pipeline_stage_closed_won
    from source,
    unnest (stages) stages
)
{% endif %}
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
