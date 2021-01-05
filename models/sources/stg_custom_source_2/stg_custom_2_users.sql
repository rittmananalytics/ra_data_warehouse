{% if not var("enable_custom_source_2") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
    select *
    from
    {{ source('custom_source_2','s_transactions' ) }}
),
renamed as (
  select
         concat('custom_1-',id)                     as user_id,
         cast(null as string)                     as user_name,
         cast(null as string)                     as user_email,
         cast(null as boolean)                    as contact_is_contractor,
         cast(null as boolean)                    as contact_is_staff,
         cast(null as numeric)                    as contact_weekly_capacity,
         cast(null as string)                     as user_phone,
         cast(null as numeric)                    as contact_default_hourly_rate,
         cast(null as numeric)                    as contact_cost_rate,
         cast(null as boolean)                    as contact_is_active,
         cast(null as timestamp)                  as user_created_ts,
         cast(null as timestamp)                  as user_last_modified_ts
  from source)
)
select * from renamed
