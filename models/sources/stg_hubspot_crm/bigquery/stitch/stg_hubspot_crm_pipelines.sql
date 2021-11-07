{{config
  (enabled =
      (target.type == 'bigquery' and var("stg_hubspot_crm_etl") == 'stitch')
   )
}}
{% if var("marketing_warehouse_deal_sources") %}
{% if 'hubspot_crm' in var("marketing_warehouse_deal_sources") %}

with source as (
  {{ filter_stitch_relation(relation=source('stitch_hubspot_crm','deal_pipelines'),unique_column='pipelineid') }}
),
renamed as (
    select
      label as pipeline_label,
      pipelineid as pipeline_id,
      displayorder as pipeline_display_order,
      active as pipeline_active
    from source
)
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
