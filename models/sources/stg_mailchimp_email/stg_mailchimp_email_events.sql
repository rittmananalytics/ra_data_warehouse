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
{{ target.database}}.{{ var('stitch_reports_email_activity_table') }})

where _sdc_batched_at = max_sdc_batched_at)
SELECT
  list_id,
  campaign_id as send_id,
  email_id as contact_id,
  timestamp,
  action,
  case when action = 'open' then 1 end as total_opens,
  case when action = 'bounce' then 1 end as total_bounces,
  case when action = 'click' then 1 end as total_clicks,
  case when action = 'open' then email_id end as contact_opened,
  case when action = 'bounce' then email_id end as contact_bounced,
  case when action = 'click' then email_id end as contact_clicked,
  type,
  email_address
from events
