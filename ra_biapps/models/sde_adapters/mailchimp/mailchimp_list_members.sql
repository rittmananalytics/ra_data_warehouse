SELECT
  _sdc_batched_at AS _sdc_batched_at,
  _sdc_received_at AS _sdc_received_at,
  _sdc_sequence AS _sdc_sequence,
  _sdc_table_version AS _sdc_table_version,
  email_address AS email_address,
  id AS list_member_id,
  ip_opt AS ip_opted_in,
  language AS language,
  last_changed AS last_changed_at,
  list_id AS list_id,
  location.country_code AS country_code,
  location.latitude AS latitude,
  location.longitude AS longitude,
  location.timezone AS timezone,
  member_rating AS member_rating,
  merge_fields.address AS address,
  merge_fields.birthday AS birthday,
  merge_fields.fname AS forename,
  merge_fields.lname AS surname,
  merge_fields.phone AS phone_number,
  stats.avg_click_rate AS avg_click_rate,
  stats.avg_open_rate AS avg_open_rate,
  status AS status,
  timestamp_opt AS opted_in_at,
  unique_email_id AS email_id
FROM
  {{ source(
    'stitch_mailchimp',
    'list_segment_members'
  ) }}
