SELECT
  _sdc_received_at AS _sdc_received_at,
  _sdc_sequence AS _sdc_sequence,
  _sdc_table_version AS _sdc_table_version,
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
  CONCAT(email_id,'_',campaign_id,'_',list_id) AS send_id
FROM
  {{ source(
    'stitch_mailchimp',
    'reports_email_activity'
  ) }}
