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
      pipelineid,
      stageid,
      probability,
      closedwon,
      stages.label as stage_label,
      stages.displayorder as stage_displayorder,
      concat (cast( pipelineid as string), cast (stageid as string)) as pk
    from source,
    unnest (stages) stages
)
select * from renamed
