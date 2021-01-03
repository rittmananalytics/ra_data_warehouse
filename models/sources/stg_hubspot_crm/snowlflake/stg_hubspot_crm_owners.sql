{{config(enabled = target.type == 'snowflake')}}
{% if var("marketing_warehouse_deal_sources") %}
{% if 'hubspot_crm' in var("marketing_warehouse_deal_sources") %}

{% if var("stg_hubspot_crm_etl") == 'fivetran' %}
WITH source as (
  select * from
  from {{ var('stg_hubspot_crm_fivetran_owners_table') }}
),
renamed as (
    select
      cast (owner_id as int) as owner_id,
      concat(concat(first_name,' '),last_name) as owner_full_name,
      first_name,
      last_name,
      email as owner_email
    from source
)
{% elif var("stg_hubspot_crm_etl") == 'stitch' %}
WITH source as (
  {{ filter_stitch_relation(relation=var('stg_hubspot_crm_stitch_owners_table'),unique_column='ownerid') }}

),
renamed as (
    select
      ownerid::STRING as owner_id,
      concat(concat(firstname,' '),lastname) as owner_full_name,
      firstname as owner_first_name,
      lastname as owner_last_name,
      email as owner_email
    from source
)
{% endif %}
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
