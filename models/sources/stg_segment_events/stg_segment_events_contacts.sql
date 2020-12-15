{% if not var("enable_mailchimp_email_source") %}
{{
    config(      enabled=false)
}}
{% endif %}

with source as (

    select * from {{ target.database}}.{{ var('stg_segment_events_segment_schema') }}.{{ var('stg_segment_events_segment_users_table') }}

)
renamed AS
(
SELECT
    select concat('stg_segment_events_id-prefix',id) AS contact_id,
    first_name AS contact_first_name,
    last_name AS contact_last_name,
    name AS contact_name,
    job_title AS contact_job_title,
    email AS contact_email,
    phone AS contact_phone,
    phone AS contact_mobile_phone,
    CAST(NULL AS STRING) AS contact_address,
    CAST(NULL AS STRING) AS contact_city,
    CAST(NULL AS STRING) AS contact_state,
    CAST(NULL AS STRING) AS contact_country,
    CAST(NULL AS STRING) AS contact_postcode_zip,
    CAST(NULL AS STRING) AS contact_company,
    CAST(NULL AS STRING) AS contact_website,
    CAST(NULL AS STRING) AS contact_company_id,
    CAST(NULL AS STRING) AS contact_owner_id,
    CAST(NULL AS STRING) AS contact_lifecycle_stage,
    cast(null as boolean)         as user_is_contractor,
    cast(null as boolean) as user_is_staff,
    cast(null as int64)           as user_weekly_capacity,
    cast(null as int64)           as user_default_hourly_rate,
    cast(null as int64)           as user_cost_rate,
    false                          as user_is_active,
    timestamp_opt AS contact_created_date,
    last_changed AS contact_last_modified_date
  FROM
    source
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26)
SELECT
  *
FROM
  renamed
