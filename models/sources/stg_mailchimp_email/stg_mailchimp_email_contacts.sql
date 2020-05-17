{% if not var("enable_mailchimp_email_source") %}
{{
    config(      enabled=false)
}}
{% endif %}

WITH source AS (SELECT
    *
  EXCEPT
    (    _sdc_batched_at,
      max_sdc_batched_at)
  FROM
    (    SELECT
        *,
        MAX(_sdc_batched_at) over (        PARTITION BY id
          ORDER BY
            _sdc_batched_at RANGE BETWEEN unbounded preceding
            AND unbounded following
        ) AS max_sdc_batched_at
      FROM
        {{ target.database}}.{{ var('stitch_list_members_table') }})
  WHERE
    _sdc_batched_at = max_sdc_batched_at),
renamed AS
(
SELECT
    id AS contact_id,
    merge_fields.fname AS contact_first_name,
    merge_fields.lname AS contact_last_name,
    CASE WHEN CONCAT(merge_fields.fname,' ',merge_fields.lname) = ' ' THEN email_address ELSE CONCAT(merge_fields.fname,' ',merge_fields.lname) END AS contact_name,
    CAST(NULL AS STRING) AS contact_job_title,
    email_address AS contact_email,
    merge_fields.phone AS contact_phone,
    CAST(NULL AS STRING) AS contact_mobile_phone,
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
    timestamp_opt AS contact_created_date,
    last_changed AS contact_last_modified_date
  FROM
    source)
SELECT
  *
FROM
  renamed
