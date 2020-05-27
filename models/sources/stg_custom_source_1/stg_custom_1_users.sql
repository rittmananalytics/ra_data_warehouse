{% if not var("enable_custom_source_1") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
    select *
    from
    {{ source('custom_source_1','s_transactions' ) }}
),
renamed as (
select
       concat('custom_1-',id)                     as user_id,
       cast(null as string)                     as user_name,
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
SELECT
  *
FROM
  renamed
