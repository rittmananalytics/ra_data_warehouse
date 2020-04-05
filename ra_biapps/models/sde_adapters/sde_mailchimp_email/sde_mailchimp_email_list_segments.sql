SELECT
  _sdc_batched_at AS _sdc_batched_at,
  _sdc_received_at AS _sdc_received_at,
  _sdc_sequence AS _sdc_sequence,
  _sdc_table_version AS _sdc_table_version,
  created_at AS created_at,
  id AS segment_id,
  list_id AS list_id,
  member_count AS member_count,
  name AS name,
  updated_at AS updated_at,
FROM
  {{ source(
    'stitch_mailchimp',
    'list_segments'
  ) }}
