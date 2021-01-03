{{config(enabled = target.type == 'snowflake')}}
{% if var("marketing_warehouse_email_event_sources") %}
{% if 'mailchimp_email' in var("marketing_warehouse_email_event_sources") %}

with source as (
  SELECT *
  FROM (
    SELECT
      *,
      MAX(_sdc_batched_at) OVER (PARTITION BY list_id,campaign_id,  email_id,  timestamp,  action,  type,  email_address ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
    FROM
      {{ var('stg_mailchimp_email_stitch_reports_email_activity_table') }})
  WHERE
    _sdc_batched_at = max_sdc_batched_at
),
joined as (
SELECT
  concat('{{ var('stg_mailchimp_email_id-prefix') }}',list_id) as list_id,
  concat('{{ var('stg_mailchimp_email_id-prefix') }}',campaign_id) as ad_campaign_id,
  concat('{{ var('stg_mailchimp_email_id-prefix') }}',email_id) as contact_id,
  timestamp as event_ts,
  action,
  type,
  email_address,
  replace(url,'[UNIQID]',email_id) as url
from source
union all
SELECT
  s.list_id,
  s.send_id as ad_campaign_id,
  c.contact_id,
  s.campaign_sent_ts as event_ts,
  'stg_enrichment_clearbit_schema' AS action,
  NULL AS type,
  c.contact_email as email_address,
  cast (null as string) as url
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
select *
from joined


{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
