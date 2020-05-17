{% if not var("enable_hubspot_crm_source")  %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
{% if var("etl") == 'fivetran' %}
with source as (
  select * from
  from {{ target.database}}.{{ var('fivetran_pipeline_stages_table') }}

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
{% elif var("etl") == 'stitch' %}
with source as (
  {{ filter_stitch_table(var('stitch_deal_pipelines_table'),'pipelineid') }}

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
