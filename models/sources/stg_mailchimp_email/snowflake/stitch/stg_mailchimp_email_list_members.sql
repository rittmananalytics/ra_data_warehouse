{{config(enabled = target.type == 'snowflake')}}
{% if var("marketing_warehouse_email_list_sources") %}
{% if 'mailchimp_email' in var("marketing_warehouse_email_list_sources") %}

WITH source as (
  {{ filter_stitch_relation(relation=var('stg_mailchimp_email_stitch_list_members_table'),unique_column='id') }}
),
renamed as (
select
  concat('{{ var('stg_mailchimp_email_id-prefix') }}',id)              as list_member_id,
  email_address   as contact_email,
  status          as list_member_status,
  source          as list_member_source,
  concat('{{ var('stg_mailchimp_email_id-prefix') }}',list_id)         as list_id,
  unsubscribe_reason as unsubscribe_reason_type,
  timestamp_opt   as list_member_ts
FROM
  source
)
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
