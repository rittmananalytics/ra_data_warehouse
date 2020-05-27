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
  from {{ target.database}}.{{ var('fivetran_pipeline_stage_table') }}
),
renamed as (
    select
      label as pipeline_label,
      pipeline_id,
      display_order as pipeline_display_order,
      active as pipeline_active
    from source
)
{% elif var("etl") == 'stitch' %}
with source as (
  {{ filter_stitch_table(var('stitch_deal_pipelines_table'),'pipelineid') }}


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
