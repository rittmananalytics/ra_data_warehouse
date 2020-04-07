WITH responses AS (
  SELECT * FROM (
  SELECT

  _sdc_batched_at AS _sdc_batched_at,
  action AS event,
  campaign_id AS campaign_id,
  email_address AS email_address,
  email_id AS email_id,
  ip AS ip,
  list_id AS list_id,
  list_is_active AS list_is_active,
  timestamp AS event_at,
  type AS bounce_type,
  url AS url,
  CONCAT(campaign_id,'_',email_id,'_',STRING(timestamp)) AS event_id,
  CONCAT(email_id,'_',campaign_id,'_',list_id) AS send_id,
  MAX(_sdc_batched_at) over (PARTITION BY CONCAT(campaign_id,'_',email_id,'_',STRING(timestamp)) ORDER BY _sdc_batched_at RANGE BETWEEN unbounded preceding AND unbounded following ) AS max_sdc_batched_at
FROM
  {{ source(
    'mailchimp_email',
    'reports_email_activity'
  ) }}
)
where _sdc_batched_at = max_sdc_batched_at)

SELECT
  *
FROM
  responses
