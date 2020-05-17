{% if not var("enable_harvest_projects_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with source as (
  {{ filter_stitch_table(var('users_table'),'id') }}

),

renamed as (
select
       concat('{{ var('id-prefix') }}',id)              as user_id,
       concat(first_name,' ',last_name)   as user_name,
       email                              as user_email,
       is_contractor                      as user_is_contractor,
       true                               as user_is_staff,
       weekly_capacity                    as user_weekly_capacity,
       telephone                          as user_phone,
       default_hourly_rate                as user_default_hourly_rate,
       cost_rate                          as user_cost_rate,
       is_active                          as user_is_active,
       created_at                         as user_created_ts,
       updated_at                         as user_last_modified_ts
from source u)
SELECT
  *
FROM
  renamed
