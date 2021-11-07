{{config(enabled = target.type == 'bigquery')}}
{% if var("marketing_warehouse_deal_sources") %}
{% if 'hubspot_crm' in var("marketing_warehouse_deal_sources") %}

with source as (
  select * from
  from {{ source('fivetran_hubspot_crm','pipeline_stages') }}

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
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
