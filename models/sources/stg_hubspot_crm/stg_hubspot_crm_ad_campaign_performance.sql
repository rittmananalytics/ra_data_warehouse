{% if not var("enable_hubspot_crm_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
with source as (
  SELECT
    ad_campaign_serve_ts,
    concat('{{ var('stg_hubspot_crm_id-prefix') }}',ad_campaign_id) as ad_campaign_id,
    NULL AS ad_campaign_budget,
    NULL AS ad_campaign_avg_cost,
    NULL AS ad_campaign_avg_time_on_site,
    safe_divide(ad_campaign_bounces,ad_campaign_total_emails_delivered) AS ad_campaign_bounce_rate,
    CAST(NULL AS string) AS ad_campaign_status,
    NULL AS ad_campaign_total_assisted_conversions,
    ad_campaign_total_emails_clicks as ad_campaign_total_clicks,
    NULL AS ad_campaign_total_conversion_value,
    NULL AS ad_campaign_total_conversions,
    NULL as ad_campaign_total_cost,
    ad_campaign_total_emails_open + ad_campaign_total_emails_clicks as ad_campaign_total_engagements,
    ad_campaign_total_emails_open as ad_campaign_total_impressions,
    ad_campaign_bounces + ad_campaign_total_emails_unsubscribed as ad_campaign_total_invalid_clicks,
    ad_network
   FROM
  {{ ref('stg_hubspot_crm_email_performance') }} )
select
  *
from
  source
