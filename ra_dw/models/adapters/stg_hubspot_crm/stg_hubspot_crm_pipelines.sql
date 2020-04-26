{% if not var("enable_hubspot_crm_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with source as (
  {{ filter_source('hubspot_crm','s_deal_pipelines','pipelineid') }}
),
renamed as (
    select
      label as pipeline_label,
      pipelineid,
      displayorder as pipeline_displayorder,
      active as pipeline_active
    from source
)
select * from renamed
