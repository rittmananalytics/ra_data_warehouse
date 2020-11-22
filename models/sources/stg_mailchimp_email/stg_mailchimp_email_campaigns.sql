{% if not var("enable_mailchimp_email_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
SELECT
  send_id AS ad_campaign_id,
  campaign_title AS ad_campaign_name,
  CASE
    WHEN campaign_is_resendable THEN 'resendable'
  ELSE
  'closed'
END
  AS ad_campaign_status,
  CAST (NULL AS string) AS campaign_buying_type,
  campaign_sent_ts AS ad_campaign_start_date,
  CAST (NULL AS timestamp) AS ad_campaign_end_date,
  'Mailchimp' AS ad_network
FROM
  {{ ref('stg_mailchimp_email_sends') }}
