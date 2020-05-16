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
  {{ source('fivetran_hubspot_crm','s_deal_pipeline_stages') }}

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
{% elif var("hubspot_crm_source_type") == 'stitch' %}
with source as (
  {{ filter_stitch_source('stitch_hubspot_crm','s_deal_pipelines','pipelineid') }}
),
renamed as (
    select
      pipelineid as pipeline_id,
      stageid as pipeline_stage_id,
      stages.label as pipeline_stage_label,
      stages.displayorder as pipeline_stage_display_order,
      probability as pipeline_stage_close_probability_pct,
      closedwon as pipeline_stage_closed_won
    from source,
    unnest (stages) stages
)
{% endif %}
select * from renamed
