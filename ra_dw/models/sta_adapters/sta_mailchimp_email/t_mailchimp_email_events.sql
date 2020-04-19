{% if not var("enable_mailchimp_email_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with events as (Select * from (
  SELECT
  *,
  MAX(_sdc_batched_at) OVER (PARTITION BY list_id,campaign_id,  email_id,  timestamp,  action,  type,  email_address ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
FROM
{{ source(
  'stitch_mailchimp',
  's_reports_email_activity'
) }})
where _sdc_batched_at = max_sdc_batched_at)
SELECT
  list_id,
  campaign_id as send_id,
  email_id as contact_id,
  timestamp,
  action,
  type,
  email_address
from events
