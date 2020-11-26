{% if not var("enable_jira_projects_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with source as (
  {{ filter_stitch_table(var('stg_jira_projects_stitch_schema'),var('stg_jira_projects_stitch_users_table'),'accountid') }}
),

renamed as
 (
  SELECT
    concat('{{ var('stg_jira_projects_id-prefix') }}',accountid)           as user_id,
    displayname                   as user_name  ,
    emailaddress                  as user_email,
    cast(null as boolean)         as user_is_contractor,
    case when emailaddress like '%@{{ var('stg_jira_projects_staff_email_domain') }}%' then true else false end as user_is_staff,
    cast(null as int64)           as user_weekly_capacity,
    cast(null as string)          as user_phone,
    cast(null as int64)           as user_default_hourly_rate,
    cast(null as int64)           as user_cost_rate,
    active                        as user_is_active,
    cast(null as timestamp)       as user_created_ts,
    cast(null as timestamp)       as user_last_modified_ts,
  FROM source
  WHERE concat('{{ var('stg_jira_projects_id-prefix') }}',accountid)  NOT LIKE '%addon%'
  UNION ALL
  SELECT
  concat('{{ var('stg_jira_projects_id-prefix') }}',-999)           as user_id,
  'Unassigned'                   as user_name  ,
  cast(null as string)                  as user_email ,
  cast(null as boolean)         as user_is_contractor,
  false as user_is_staff,
  cast(null as int64)           as user_weekly_capacity,
  cast(null as string)          as user_phone,
  cast(null as int64)           as user_default_hourly_rate,
  cast(null as int64)           as user_cost_rate,
  false                          as user_is_active,
  cast(null as timestamp)       as user_created_ts,
  cast(null as timestamp)       as user_last_modified_ts
)
SELECT
 *
FROM
 renamed
