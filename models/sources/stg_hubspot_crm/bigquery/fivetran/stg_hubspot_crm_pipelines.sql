{% if target.type == 'bigquery' %}
{% if var("marketing_warehouse_deal_sources") %}
{% if 'hubspot_crm' in var("marketing_warehouse_deal_sources") %}
{% if var("stg_hubspot_crm_etl") == 'fivetran' %}

with source as (
  select *
  from {{ source('fivetran_hubspot_crm','pipelines') }}
),
renamed as (
    select
      label as pipeline_label,
      pipeline_id,
      display_order as pipeline_display_order,
      active as pipeline_active
    from source
)
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
