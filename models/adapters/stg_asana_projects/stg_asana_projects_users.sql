{% if not var("enable_asana_projects_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
  {{ filter_stitch_table(var('users_table'),'gid') }}
  ),

renamed AS (
  SELECT
  concat('{{ var('id-prefix') }}',gid)           as user_id,
  name                   as user_name  ,
  email                  as user_email ,
  cast(null as boolean)         as user_is_contractor,
  case when email like '%@{{ var('staff_email_domain') }}%' then true else false end as user_is_staff,
  cast(null as int64)           as user_weekly_capacity,
  cast(null as string)          as user_phone,
  cast(null as int64)           as user_default_hourly_rate,
  cast(null as int64)           as user_cost_rate,
  true                          as user_is_active,
  cast(null as timestamp)       as user_created_ts,
  cast(null as timestamp)       as user_last_modified_ts
  FROM
    source
  WHERE
    name NOT LIKE 'Private User'
)
SELECT
  *
FROM
  renamed
