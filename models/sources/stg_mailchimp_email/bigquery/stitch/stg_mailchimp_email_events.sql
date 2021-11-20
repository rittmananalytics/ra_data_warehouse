{{config(enabled = target.type == 'bigquery')}}
{% if var("marketing_warehouse_email_event_sources") %}
{% if 'mailchimp_email' in var("marketing_warehouse_email_event_sources") %}

with source AS (
  SELECT *
  FROM (
    SELECT
      *,
      MAX(_sdc_batched_at) OVER (PARTITION BYlist_id,campaign_id,  email_id,  timestamp,  action,  type,  email_address ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
    FROM
      {{ source('stitch_mailchimp_email', 'email_activity') }})
  WHERE
    _sdc_batched_at = max_sdc_batched_at
),
joined AS (
SELECT
  CONCAT('{{ var('stg_mailchimp_email_id-prefix') }}',list_id) AS list_id,
  CONCAT('{{ var('stg_mailchimp_email_id-prefix') }}',campaign_id) AS ad_campaign_id,
  CONCAT('{{ var('stg_mailchimp_email_id-prefix') }}',email_id) AS contact_id,
  timestamp AS event_ts,
  action,
  type,
  email_address,
  replace(url,'[UNIQID]',email_id) AS url
FROM source
union all
SELECT
  s.list_id,
  s.send_id AS ad_campaign_id,
  c.contact_id,
  s.campaign_sent_ts AS event_ts,
  'stg_enrichment_clearbit_schema' AS action,
  NULL AS type,
  c.contact_email AS email_address,
  CAST(null AS {{ dbt_utils.type_string() }}) AS url
FROM
  {{ ref('stg_mailchimp_email_sends') }}  s
JOIN
  {{ ref('stg_mailchimp_email_list_members') }} m
ON
  s.list_id = m.list_id
JOIN
  {{ ref('stg_mailchimp_email_contacts') }} c
ON
  c.contact_email = m.contact_email)
SELECT *
FROM joined


{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
