{% if not var("enable_jira_projects_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with source as (
  SELECT
    * EXCEPT (_sdc_batched_at, max_sdc_batched_at)
  FROM (
    SELECT
      *,
      MAX(_sdc_batched_at) OVER (PARTITION BY key ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
    FROM
      {{ target.database}}.{{ var('stitch_users_table') }})
  WHERE
    max_sdc_batched_at = _sdc_batched_at
),

renamed as
 (
  SELECT
    concat('{{ var('id-prefix') }}',key)           as user_id,
    displayname                   as user_name  ,
    emailaddress                  as user_email,
    cast(null as boolean)         as user_is_contractor,
    case when emailaddress like '%@{{ var('staff_email_domain') }}%' then true else false end as user_is_staff,
    cast(null as int64)           as user_weekly_capacity,
    cast(null as string)          as user_phone,
    cast(null as int64)           as user_default_hourly_rate,
    cast(null as int64)           as user_cost_rate,
    active                        as user_is_active,
    cast(null as timestamp)       as user_created_ts,
    cast(null as timestamp)       as user_last_modified_ts,
  FROM source
  WHERE concat('{{ var('id-prefix') }}',key)  NOT LIKE '%addon%')
SELECT
 *
FROM
 renamed
