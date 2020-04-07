with list_membership as (
  SELECT * except (_sdc_batched_at, max_sdc_batched_at)
  FROM (
SELECT
  email_address AS email_address,
  listmembers.id AS list_member_id,
  list_id AS list_id,
  ip_opt AS ip_opted_in,
  language AS language,
  last_changed AS last_changed_at,
  member_rating AS member_rating,
  stats.avg_click_rate AS avg_click_rate,
  stats.avg_open_rate AS avg_open_rate,
  status AS status,
  timestamp_opt AS opted_in_at,
  unique_email_id AS email_id,
  unsubscribe_reason AS unsubscribe_reason,
  _sdc_batched_at AS _sdc_batched_at,
  MAX(_sdc_batched_at) over (PARTITION BY listmembers.id ORDER BY _sdc_batched_at RANGE BETWEEN unbounded preceding AND unbounded following ) AS max_sdc_batched_at
FROM
  {{ source(
    'mailchimp_email','list_members'
  ) }} AS listmembers,
  UNNEST(tags) AS Tags )
WHERE _sdc_batched_at = max_sdc_batched_at)
select * from list_membership
