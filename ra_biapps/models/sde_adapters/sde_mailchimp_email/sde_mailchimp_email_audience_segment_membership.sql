WITH segment_members AS (
  SELECT
    *
  FROM
    {{ ref('sde_mailchimp_email_audience_list') }}
),
select_segment_members AS (
  SELECT
    _sdc_batched_at,
    email_address,
    list_member_id,
    list_id,
    last_changed_at AS valid_from,
    segment_id,
    opted_in_at,
    email_id,
    unsubscribe_reason,
    MAX(_sdc_batched_at) over (PARTITION BY list_member_id, last_changed_at ORDER BY _sdc_batched_at RANGE BETWEEN unbounded preceding AND unbounded following) AS valid_to
  FROM
    segment_members
),
latest_version AS (
  SELECT
    *
  FROM
    select_segment_members
  WHERE
    _sdc_batched_at = valid_to
)
SELECT
  *
FROM
  latest_version
