{{config(enabled = target.type == 'bigquery')}}
{% if var("marketing_warehouse_email_event_sources") %}
{% if 'mailchimp_email' in var("marketing_warehouse_email_event_sources") %}

WITH source AS (
  SELECT *
  FROM {{ var('stg_mailchimp_email_stitch_campaigns_table') }}
),
renamed as (
SELECT
  concat('{{ var('stg_mailchimp_email_id-prefix') }}',id) AS send_id,
  content_type AS campaign_content_type,
  create_time AS campaign_created_at_ts,
  emails_sent AS total_campaign_emails_sent,
  long_archive_url AS campaign_archive_url,
  concat('{{ var('stg_mailchimp_email_id-prefix') }}',recipients.list_id) AS list_id,
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
  tracking.text_clicks AS campaign_tracking_text_clicks
FROM
  source)
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
