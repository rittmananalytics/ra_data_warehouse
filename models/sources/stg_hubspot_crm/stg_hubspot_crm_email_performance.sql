{% if not var("enable_hubspot_crm_source")  %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
{% if var("stg_hubspot_crm_etl") == 'stitch' %}
with source as (
  SELECT
    concat('{{ var('stg_hubspot_crm_id-prefix') }}',contentid) as ad_campaign_id,
    timestamp(DATE(_sdc_received_at)) AS ad_campaign_serve_ts,
    coalesce(numincluded,0)-coalesce(lag(numincluded) over (partition by contentid order by _sdc_received_at),0) as ad_campaign_total_audience,
    coalesce(counters.processed,0)-coalesce(lag(counters.processed) over (partition by contentid order by _sdc_received_at),0) as ad_campaign_emails_processed,
    coalesce(counters.bounce,0)-coalesce(lag(counters.bounce) over (partition by contentid order by _sdc_received_at),0) as ad_campaign_bounces,
    coalesce(counters.delivered,0)-coalesce(lag(counters.delivered) over (partition by contentid order by _sdc_received_at),0) as ad_campaign_total_emails_delivered,
    coalesce(counters.sent,0)-coalesce(lag(counters.sent) over (partition by contentid order by _sdc_received_at),0) as ad_campaign_total_emails_sent,
    coalesce(counters.open,0)-coalesce(lag(counters.open) over (partition by contentid order by _sdc_received_at),0) as ad_campaign_total_emails_open,
    coalesce(counters.deferred,0)-coalesce(lag(counters.deferred) over (partition by contentid order by _sdc_received_at),0) as ad_campaign_total_emails_deferred,
    coalesce(counters.dropped,0)-coalesce(lag(counters.dropped) over (partition by contentid order by _sdc_received_at),0) as ad_campaign_total_emails_dropped,
    coalesce(counters.click,0)-coalesce(lag(counters.click) over (partition by contentid order by _sdc_received_at),0) as ad_campaign_total_emails_clicks,
    coalesce(counters.unsubscribed,0)-coalesce(lag(counters.unsubscribed) over (partition by contentid order by _sdc_received_at),0) as ad_campaign_total_emails_unsubscribed,
    'Hubspot Email' as ad_network
  FROM
  (
  select *, max(_sdc_received_at) over (partition by contentid, date(_sdc_received_at)) as max_sdc_received_at_for_day
  from
  {{ target.database}}.{{ var('stg_hubspot_crm_stitch_schema') }}.{{ var('stg_hubspot_crm_stitch_campaigns_table') }})
  where _sdc_received_at = max_sdc_received_at_for_day
ORDER BY
  id, _sdc_received_at
)
{% endif %}
select * from source
