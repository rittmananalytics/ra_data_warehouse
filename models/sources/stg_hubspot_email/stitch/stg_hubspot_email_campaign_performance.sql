{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("marketing_warehouse_email_event_sources") %}
{% if 'hubspot_email' in var("marketing_warehouse_email_event_sources") %}
{% if var("stg_xero_accounting_etl") == 'stitch' %}

with source AS (
  SELECT
    ad_campaign_serve_ts,
    ad_campaign_id AS ad_campaign_id,
    NULL AS ad_campaign_budget,
    NULL AS ad_campaign_avg_cost,
    NULL AS ad_campaign_avg_time_on_site,
    {{ dbt_utils.safe_divide('ad_campaign_bounces','ad_campaign_total_emails_delivered')}} AS ad_campaign_bounce_rate,
    CAST(null AS {{ dbt_utils.type_string() }}) AS ad_campaign_status,
    NULL AS ad_campaign_total_assisted_conversions,
    ad_campaign_total_emails_clicks AS ad_campaign_total_clicks,
    NULL AS ad_campaign_total_conversion_value,
    NULL AS ad_campaign_total_conversions,
    NULL AS ad_campaign_total_cost,
    ad_campaign_total_emails_open + ad_campaign_total_emails_clicks AS ad_campaign_total_engagements,
    ad_campaign_total_emails_open AS ad_campaign_total_impressions,
    ad_campaign_bounces + ad_campaign_total_emails_unsubscribed AS ad_campaign_total_invalid_clicks,
    ad_network
   FROM
  {{ source('stitch_hubspot_email','email_performance') }} )
SELECT
  *
from
  source

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
