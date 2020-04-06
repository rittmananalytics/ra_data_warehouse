WITH lists AS (
  SELECT * FROM
  (
  SELECT
  campaign_defaults.from_email AS default_from_email,
  campaign_defaults.from_name AS default_from_name,
  campaign_defaults.LANGUAGE AS default_language,
  campaign_defaults.subject AS default_subject,
  contact.address1 AS default_from_address1,
  contact.address2 AS default_from_address2,
  contact.city AS default_from_city,
  contact.company AS default_from_company,
  contact.country AS default_from_country,
  contact.phone AS default_from_phone,
  contact.state AS default_from_state,
  contact.zip AS default_from_zip,
  id AS list_id,
  name AS name,
  stats.avg_sub_rate AS avg_sub_rate,
  stats.avg_unsub_rate AS avg_unsub_rate,
  stats.campaign_count AS campaign_count,
  stats.campaign_last_sent AS campaign_last_sent,
  stats.cleaned_count AS cleaned_count,
  stats.cleaned_count_since_send AS cleaned_count_since_send,
  stats.click_rate AS click_rate,
  stats.last_sub_date AS last_sub_date,
  stats.last_unsub_date AS last_unsub_date,
  stats.member_count AS member_count,
  stats.member_count_since_send AS member_count_since_send,
  stats.merge_field_count AS merge_field_count,
  stats.open_rate AS open_rate,
  stats.target_sub_rate AS target_sub_rate,
  stats.unsubscribe_count AS unsubscribe_count,
  stats.unsubscribe_count_since_send AS unsubscribe_count_since_send,
  subscribe_url_long AS subscribe_url_long,
  visibility AS visibility,
  _sdc_batched_at AS _sdc_batched_at,
  MAX(_sdc_batched_at) over (PARTITION BY list_id ORDER BY _sdc_batched_at RANGE BETWEEN unbounded preceding AND unbounded following ) AS max_sdc_batched_at

FROM
  {{ source('mailchimp_email','lists') }} )
  WHERE
    _sdc_batched_at = max_sdc_batched_at
),
select_lists AS (
  SELECT
    list_id,
    default_from_email,
    default_from_name,
    default_language,
    default_subject,
    default_from_address1,
    default_from_address2,
    default_from_city,
    default_from_company,
    default_from_country,
    default_from_phone,
    default_from_state,
    default_from_zip,
    name,
    last_sub_date,
    last_unsub_date,
    member_count,
    member_count_since_send,
    open_rate,
    target_sub_rate,
    unsubscribe_count,
    unsubscribe_count_since_send,
    subscribe_url_long,
    visibility
    FROM lists
