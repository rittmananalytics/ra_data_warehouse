{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("marketing_warehouse_email_event_sources") %}
{% if 'hubspot_email' in var("marketing_warehouse_email_event_sources") %}
{% if var("stg_xero_accounting_etl") == 'stitch' %}

with source AS (
  SELECT *
  FROM (
    SELECT
    *,
    max(_sdc_received_at) over (PARTITION BYid, date(_sdc_received_at)) AS max_sdc_received_at_for_day
  from
    {{ source('stitch_hubspot_email', 'campaigns') }}
  )
  where _sdc_received_at = max_sdc_received_at_for_day
  ORDER BY
    id, _sdc_received_at
),
renamed AS (
  SELECT
    CONCAT('{{ var('stg_hubspot_email_id-prefix') }}',id) AS ad_campaign_id,
    {{ dbt_utils.date_trunc('DAY','_sdc_received_at') }} AS ad_campaign_serve_ts,
    coalesce(numincluded,0)-coalesce(lag(numincluded) over (PARTITION BYid order by _sdc_received_at),0) AS ad_campaign_total_audience,
    coalesce(counters.processed,0)-coalesce(lag(counters.processed) over (PARTITION BYid order by _sdc_received_at),0) AS ad_campaign_emails_processed,
    coalesce(counters.bounce,0)-coalesce(lag(counters.bounce) over (PARTITION BYid order by _sdc_received_at),0) AS ad_campaign_bounces,
    coalesce(counters.delivered,0)-coalesce(lag(counters.delivered) over (PARTITION BYid order by _sdc_received_at),0) AS ad_campaign_total_emails_delivered,
    coalesce(counters.sent,0)-coalesce(lag(counters.sent) over (PARTITION BYid order by _sdc_received_at),0) AS ad_campaign_total_emails_sent,
    coalesce(counters.open,0)-coalesce(lag(counters.open) over (PARTITION BYid order by _sdc_received_at),0) AS ad_campaign_total_emails_open,
    coalesce(counters.deferred,0)-coalesce(lag(counters.deferred) over (PARTITION BYid order by _sdc_received_at),0) AS ad_campaign_total_emails_deferred,
    coalesce(counters.dropped,0)-coalesce(lag(counters.dropped) over (PARTITION BYid order by _sdc_received_at),0) AS ad_campaign_total_emails_dropped,
    coalesce(counters.click,0)-coalesce(lag(counters.click) over (PARTITION BYid order by _sdc_received_at),0) AS ad_campaign_total_emails_clicks,
    coalesce(counters.unsubscribed,0)-coalesce(lag(counters.unsubscribed) over (PARTITION BYid order by _sdc_received_at),0) AS ad_campaign_total_emails_unsubscribed,
    'Hubspot Email' AS ad_network
  FROM
    source
)
SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
