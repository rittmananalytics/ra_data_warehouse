{% if target.type == 'bigquery' %}
{% if var("marketing_warehouse_deal_sources") %}
{% if 'hubspot_crm' in var("marketing_warehouse_deal_sources") %}
{% if var("stg_hubspot_crm_etl") == 'fivetran' %}

WITH source as (
  select *
  from {{ source('fivetran_hubspot_crm','owners') }}
),
renamed as (
    select
      safe_cast (owner_id as int64) as owner_id,
      concat(concat(first_name,' '),last_name) as owner_full_name,
      first_name,
      last_name,
      email as owner_email
    from source
)
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
