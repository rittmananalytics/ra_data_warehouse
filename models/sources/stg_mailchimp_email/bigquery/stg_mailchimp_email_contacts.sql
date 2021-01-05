{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contact_sources") %}
{% if 'mailchimp_email' in var("crm_warehouse_contact_sources") %}

WITH source AS (
  {{ filter_stitch_relation(relation=var('stg_mailchimp_email_stitch_list_members_table'),unique_column='id') }}
),
renamed AS
(
SELECT
    concat('{{ var('stg_mailchimp_email_id-prefix') }}',id) AS contact_id,
    merge_fields.fname AS contact_first_name,
    merge_fields.lname AS contact_last_name,
    CASE WHEN CONCAT(merge_fields.fname,' ',merge_fields.lname) = ' ' THEN email_address ELSE CONCAT(merge_fields.fname,' ',merge_fields.lname) END AS contact_name,
    CAST(NULL AS STRING) AS contact_job_title,
    email_address AS contact_email,
    merge_fields.phone AS contact_phone,
    merge_fields.address__re.addr1 AS contact_address,
    merge_fields.address__re.city AS contact_city,
    merge_fields.address__re.state AS contact_state,
    merge_fields.address__re.country AS contact_country,
    merge_fields.address__re.zip AS contact_postcode_zip,
    CAST(NULL AS STRING) AS contact_company,
    CAST(NULL AS STRING) AS contact_website,
    CAST(NULL AS STRING) AS contact_company_id,
    CAST(NULL AS STRING) AS contact_owner_id,
    status AS contact_lifecycle_stage,
    cast(null as boolean)         as contact_is_contractor,
    cast(null as boolean) as contact_is_staff,
    cast(null as int64)           as contact_weekly_capacity,
    cast(null as int64)           as contact_default_hourly_rate,
    cast(null as int64)           as contact_cost_rate,
    false                          as contact_is_active,
    timestamp_opt AS contact_created_date,
    last_changed AS contact_last_modified_date
  FROM
    source)
SELECT
  *
FROM
  renamed

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
