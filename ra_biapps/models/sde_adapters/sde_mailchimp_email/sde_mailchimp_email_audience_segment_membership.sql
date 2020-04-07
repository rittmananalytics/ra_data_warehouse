WITH audience_list_members AS (
  SELECT * FROM (
  SELECT
    email_address,
    list_member_id,
    list_id,
    last_changed_at AS valid_from,
    segment_id,
    opted_in_at,
    email_id,
    unsubscribe_reason,
    _sdc_batched_at,
    MAX(_sdc_batched_at) over (PARTITION BY list_member_id, last_changed_at ORDER BY _sdc_batched_at RANGE BETWEEN unbounded preceding AND unbounded following) AS valid_to
  FROM
    {{ ref('sde_mailchimp_email_audience_list') }})
    WHERE
      _sdc_batched_at = valid_to
)
SELECT * FROM audience_list_members
