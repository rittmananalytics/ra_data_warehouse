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
      label as pipeline_label,
      pipeline_id,
      display_order as pipeline_display_order,
      active as pipeline_active
    from source
)
{% elif var("stg_hubspot_crm_etl") == 'stitch' %}
with source as (
  {{ filter_stitch_relation(relation=var('stg_hubspot_crm_stitch_deal_pipelines_table'),unique_column='pipelineid') }}
),
renamed as (
    select
      label as pipeline_label,
      pipelineid as pipeline_id,
      displayorder as pipeline_display_order,
      active as pipeline_active
    from source
)
{% endif %}
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
