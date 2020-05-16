{% if not var("enable_hubspot_crm_source")  %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
{% if var("hubspot_crm_source_type") == 'fivetran' %}
with source as (
  select * from
  {{ source('fivetran_hubspot_crm','s_deal_pipeline') }}
),
renamed as (
    select
      label as pipeline_label,
      pipeline_id,
      display_order as pipeline_display_order,
      active as pipeline_active
    from source
)
{% elif var("hubspot_crm_source_type") == 'stitch' %}
with source as (
  {{ filter_stitch_source('stitch_hubspot_crm','s_deal_pipelines','pipelineid') }}
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
