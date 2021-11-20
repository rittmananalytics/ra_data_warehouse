{{config(enabled = target.type == 'bigquery')}}
{% if var("marketing_warehouse_email_list_sources") %}
{% if 'mailchimp_email' in var("marketing_warehouse_email_list_sources") %}

WITH source AS (
  {{ filter_stitch_relation(relation=source('stitch_mailchimp_email', 'list_members') ,unique_column='id') }}
),
renamed AS (
SELECT
  CONCAT('{{ var('stg_mailchimp_email_id-prefix') }}',id)              AS list_member_id,
  email_address   AS contact_email,
  status          AS list_member_status,
  source          AS list_member_source,
  CONCAT('{{ var('stg_mailchimp_email_id-prefix') }}',list_id)         AS list_id,
  unsubscribe_reason AS unsubscribe_reason_type,
  timestamp_opt   AS list_member_ts
FROM
  source
)
SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
