{% if not var("enable_looker_usage_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source as (
    select name
    from
    {{ target.database}}.{{ var('fivetran_schema') }}.{{ var('fivetran_usage_table') }}
),
renamed as (
  select
         concat('{{ var('id-prefix') }}',id)      as user_id,
         name                                     as user_name,
         cast(null as string)                     as user_email,
         cast(null as boolean)                    as user_is_contractor,
         cast(null as boolean)                    as user_is_staff,
         cast(null as numeric)                    as user_weekly_capacity,
         cast(null as string)                     as user_phone,
         cast(null as numeric)                    as user_default_hourly_rate,
         cast(null as numeric)                    as user_cost_rate,
         cast(null as boolean)                    as user_is_active,
         cast(null as timestamp)                  as user_created_ts,
         cast(null as timestamp)                  as user_last_modified_ts
  from source)
)
select * from renamed
