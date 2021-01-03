{{config(enabled = target.type == 'snowflake')}}
{% if var("marketing_warehouse_deal_sources") %}
{% if 'hubspot_crm' in var("marketing_warehouse_deal_sources") %}

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
      stages:stageid::STRING as pipeline_stage_id,
      stages:label::STRING as pipeline_stage_label,
      stages:displayorder::INT as pipeline_stage_display_order,
      stages:probability::FLOAT pipeline_stage_close_probability_pct,
      stages:closedwon::BOOLEAN as pipeline_stage_closed_won
    from source
)
{% endif %}
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
