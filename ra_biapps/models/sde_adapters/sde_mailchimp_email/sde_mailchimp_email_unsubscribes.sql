SELECT
  _sdc_received_at AS _sdc_received_at,
  _sdc_sequence AS _sdc_sequence,
  _sdc_table_version AS _sdc_table_version,
  campaign_id AS campaign_id,
  email_address AS email_address,
  email_id AS email_id,
  list_id AS list_id,
  reason AS unsubscribe_reason,
  timestamp AS unsubscribed_at
FROM
  {{ source(
    'stitch_mailchimp',
    'unsubscribes'
  ) }}
