{% if not var("enable_mailchimp_email_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH campaigns AS (
  SELECT * except (_sdc_batched_at, max_sdc_batched_at)
  FROM (
SELECT
  id AS send_id,
  content_type AS campaign_content_type,
  create_time AS campaign_created_at_ts,
  emails_sent AS total_campaign_emails_sent,
  long_archive_url AS campaign_archive_url,
  recipients.list_id AS list_id,
  recipients.list_is_active AS campaign_list_is_active,
  recipients.list_name AS list_name,
  recipients.recipient_count AS total_recipient_count,
  report_summary.click_rate AS click_rate_pct,
  report_summary.clicks AS total_clicks,
  report_summary.open_rate AS open_rate_pct,
  report_summary.opens AS total_opens,
  report_summary.subscriber_clicks AS total_subscriber_clicks,
  report_summary.unique_opens AS total_unique_opens,
  resendable AS campaign_is_resendable,
  send_time AS campaign_sent_ts,
  settings.subject_line AS campaign_subject_line,
  settings.title AS campaign_title,
  status AS campaign_status,
  tracking.html_clicks AS campaign_tracking_html_clicks,
  tracking.opens AS campaign_tracking_opens,
  tracking.text_clicks AS campaign_tracking_text_clicks,
  _sdc_batched_at,
  MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
  FROM
  {{ target.database}}.{{ var('stitch_campaigns_table') }})

  WHERE
  _sdc_batched_at = max_sdc_batched_at)
select * from campaigns
