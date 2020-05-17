{% if not var("enable_hubspot_crm_source")  %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
{% if var("etl") == 'fivetran' %}
WITH source as (
  select * from
  from {{ target.database}}.{{ var('fivetran_owner_table') }}
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
{% elif var("etl") == 'stitch' %}
WITH source as (
  {{ filter_stitch_table(var('stitch_owners_table'),'ownerid') }}

),
renamed as (
    select
      safe_cast (ownerid as int64) as owner_id,
      concat(concat(firstname,' '),lastname) as owner_full_name,
      firstname as owner_first_name,
      lastname as owner_last_name,
      email as owner_email
    from source
)
{% endif %}
select * from renamed
