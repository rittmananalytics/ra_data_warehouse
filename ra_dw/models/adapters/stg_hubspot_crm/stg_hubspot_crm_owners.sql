{% if not var("enable_hubspot_crm_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
WITH source as (
  {{ filter_source('hubspot_crm','s_owners','ownerid') }}
),
renamed as (
    select
      safe_cast (ownerid as int64) as ownerid,
      concat(concat(firstname,' '),lastname) as owner_fullname,
      firstname,
      lastname,
      concat(concat(firstname,' '),lastname) as salesperson_full_name,
      email,
    from source
)
select * from renamed
