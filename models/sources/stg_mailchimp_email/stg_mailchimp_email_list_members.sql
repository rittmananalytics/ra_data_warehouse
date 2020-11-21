{% if not var("enable_mailchimp_email_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source as (

  {{ filter_stitch_table(var('stg_mailchimp_email_stitch_schema'),var('stg_mailchimp_email_stitch_list_members_table'),'id') }}

),
renamed as (
select
  id              as list_member_id,
  email_address   as contact_email,
  status          as list_member_status,
  source          as list_member_source,
  list_id         as list_id,
  unsubscribe_reason as unsubscribe_reason_type,
  timestamp_opt   as list_member_ts
FROM
  source
)
select * from renamed
