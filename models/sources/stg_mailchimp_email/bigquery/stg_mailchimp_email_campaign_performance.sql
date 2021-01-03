{{config(enabled = target.type == 'bigquery')}}
{% if var("marketing_warehouse_email_event_sources") %}
{% if 'mailchimp_email' in var("marketing_warehouse_email_event_sources") %}

with source as (
  select * from (
    SELECT
      id,
      concat('mailchimp-',id) AS ad_campaign_id,
      content_type AS campaign_content_type,
      create_time AS campaign_created_at_ts,
      emails_sent AS total_campaign_emails_sent,
      long_archive_url AS campaign_archive_url,
      concat('mailchimp-',recipients.list_id) AS list_id,
      recipients.list_is_active AS campaign_list_is_active,
      recipients.list_name AS list_name,
      recipients.recipient_count AS total_recipient_count,
      report_summary.click_rate AS click_rate_pct,
      report_summary.clicks AS total_clicks,
      report_summary.unique_opens AS total_unique_opens,
      report_summary.open_rate AS open_rate_pct,
      report_summary.opens AS total_opens,
      report_summary.subscriber_clicks AS total_subscriber_clicks,
      resendable AS campaign_is_resendable,
      {{ dbt_utils.date_trunc('DAY','send_time') }} AS ad_campaign_serve_ts,
      settings.subject_line AS campaign_subject_line,
      settings.title AS campaign_title,
      status AS campaign_status,
      tracking.html_clicks AS campaign_tracking_html_clicks,
      tracking.opens AS campaign_tracking_opens,
      tracking.text_clicks AS campaign_tracking_text_clicks,
      _sdc_batched_at,
      max(_sdc_batched_at) over (partition by id order by _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as max_sdc_batched_at,
    FROM `ra-development.stitch_mailchimp.campaigns`
    {{ dbt_utils.group_by(25) }})
    where _sdc_batched_at = max_sdc_batched_at
),
renamed as
  (
  SELECT
    ad_campaign_serve_ts,
    ad_campaign_id,
    NULL AS ad_campaign_budget,
    NULL AS ad_campaign_avg_cost,
    NULL AS ad_campaign_avg_time_on_site,
    NULL AS ad_campaign_bounce_rate,
    CAST(NULL AS string) AS ad_campaign_status,
    NULL AS ad_campaign_total_assisted_conversions,
    total_clicks AS ad_campaign_total_clicks,
    NULL AS ad_campaign_total_conversion_value,
    NULL AS ad_campaign_total_conversions,
    total_recipient_count*0.01642 as ad_campaign_total_cost,
    total_unique_opens as ad_campaign_total_engagements,
    total_campaign_emails_sent	 as ad_campaign_total_impressions,
    NULL AS ad_campaign_total_invalid_clicks,
   'Mailchimp' AS ad_network
    FROM
      source)
select
  *
from
  renamed

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
