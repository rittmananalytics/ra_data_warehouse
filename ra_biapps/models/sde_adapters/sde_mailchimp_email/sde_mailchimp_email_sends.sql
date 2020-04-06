WITH
campaigns AS (
  SELECT
    *
  FROM
    {{ ref('sde_mailchimp_email_campaigns') }}
),

list_members AS (
  SELECT
    *
  FROM
    {{ ref('sde_mailchimp_email_audience_list') }}
),

responses AS (
  SELECT SUM(CASE WHEN responses.event = 'bounce' THEN 1 ELSE 0 END) AS count_bounces,
  SUM(CASE WHEN responses.event = 'open' THEN 1 ELSE 0 END) AS count_opens,
  SUM(CASE WHEN responses.event = 'click' THEN 1 ELSE 0 END) AS count_clicks,
  MAX(email_id),
  send_id
  FROM (
  SELECT
  *,
  MAX(_sdc_batched_at) over (PARTITION BY event_id ORDER BY _sdc_batched_at RANGE BETWEEN unbounded preceding AND unbounded following ) AS max_sdc_batched_at
  FROM
  {{ source('mailchimp_email','reports_email_activity') }})
    WHERE
      _sdc_batched_at = max_sdc_batched_at
    GROUP BY send_id
),

sends AS (
  SELECT
    CONCAT(
    listmembers.list_member_id,'_',campaigns.campaign_id,'_',campaigns.list_id
    ) AS send_id,
    listmembers.email_id,
    campaigns.campaign_id,
    campaigns._sdc_batched_at,
    campaigns.created_at,
    campaigns.number_emails_sent,
    campaigns.archive_url,
    campaigns.list_id,
    campaigns.list_is_active,
    campaigns.list_name,
    campaigns.recipient_count,
    campaigns.sent_at,
    campaigns.has_authenticate,
    campaigns.has_auto_footer,
    campaigns.has_auto_tweet,
    campaigns.is_drag_and_drop,
    campaigns.has_fb_comments,
    campaigns.from_name,
    campaigns.preview_text,
    campaigns.reply_to,
    campaigns.subject_line,
    campaigns.template_id,
    campaigns.title,
    campaigns.to_name,
    campaigns.status
  FROM
    campaigns
    INNER JOIN list_members AS listmembers ON campaigns.list_id = listmembers.list_id
  WHERE
    campaigns.sent_at IS NOT NULL
),

send_stats AS (
  SELECT
    sends.*,
    COALESCE(latest_response_counts.count_bounces,0) AS count_bounces,
    COALESCE(latest_response_counts.count_opens,0)  AS count_opens,
    COALESCE(latest_response_counts.count_clicks,0) AS count_clicks
  FROM sends
  LEFT JOIN
  latest_response_counts ON
  sends.send_id = latest_response_counts.send_id
)

SELECT
  *
FROM
  send_stats
