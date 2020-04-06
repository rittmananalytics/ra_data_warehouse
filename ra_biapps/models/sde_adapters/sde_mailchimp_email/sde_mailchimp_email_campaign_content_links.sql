WITH links AS (
  SELECT * except (_sdc_batched_at, max_sdc_batched_at)
  FROM (
  SELECT
    url,
    campaign_id,
    _sdc_batched_at,
    MAX(_sdc_batched_at) over (PARTITION BY event_id ORDER BY _sdc_batched_at RANGE BETWEEN unbounded preceding AND unbounded following ) AS max_sdc_batched_at
  FROM
  {{ source(
  'mailchimp_email',
  'reports_email_activity'
) }}
  WHERE url IS NOT NULL )
  WHERE _sdc_batched_at = max_sdc_batched_at
),
SELECT
  *
FROM
  links
