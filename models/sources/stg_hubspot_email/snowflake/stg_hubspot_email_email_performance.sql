{{config(enabled = target.type == 'snowflake')}}
{% if var("marketing_warehouse_email_event_sources") %}
{% if 'hubspot_email' in var("marketing_warehouse_email_event_sources") %}

with source as (
  SELECT *
  FROM (
    SELECT
    *,
    max(_sdc_received_at) over (partition by id, date(_sdc_received_at)) as max_sdc_received_at_for_day
  from
    {{ var('stg_hubspot_email_stitch_campaigns_table') }}
  )
  where _sdc_received_at = max_sdc_received_at_for_day
  ORDER BY
    id, _sdc_received_at
),
renamed as (
  SELECT
    concat('{{ var('stg_hubspot_email_id-prefix') }}',id) as ad_campaign_id,
    {{ dbt_utils.date_trunc('DAY','_sdc_received_at::TIMESTAMP') }} AS ad_campaign_serve_ts,
    coalesce(numincluded,0)-coalesce(lag(numincluded) over (partition by id order by _sdc_received_at),0) as ad_campaign_total_audience,
    coalesce(counters:processed::INT,0)-coalesce(lag(counters:processed::INT) over (partition by id order by _sdc_received_at),0) as ad_campaign_emails_processed,
    coalesce(counters:bounce::INT,0)-coalesce(lag(counters:bounce::INT) over (partition by id order by _sdc_received_at),0) as ad_campaign_bounces,
    coalesce(counters:delivered::INT,0)-coalesce(lag(counters:delivered::INT) over (partition by id order by _sdc_received_at),0) as ad_campaign_total_emails_delivered,
    coalesce(counters:sent::INT,0)-coalesce(lag(counters:sent::INT) over (partition by id order by _sdc_received_at),0) as ad_campaign_total_emails_sent,
    coalesce(counters:open::INT,0)-coalesce(lag(counters:open::INT) over (partition by id order by _sdc_received_at),0) as ad_campaign_total_emails_open,
    coalesce(counters:deferred::INT,0)-coalesce(lag(counters:deferred::INT) over (partition by id order by _sdc_received_at),0) as ad_campaign_total_emails_deferred,
    coalesce(counters:dropped::INT,0)-coalesce(lag(counters:dropped::INT) over (partition by id order by _sdc_received_at),0) as ad_campaign_total_emails_dropped,
    coalesce(counters:click::INT,0)-coalesce(lag(counters:click::INT) over (partition by id order by _sdc_received_at),0) as ad_campaign_total_emails_clicks,
    coalesce(counters:unsubscribed::INT,0)-coalesce(lag(counters:unsubscribed::INT) over (partition by id order by _sdc_received_at),0) as ad_campaign_total_emails_unsubscribed,
    'Hubspot Email' as ad_network
  FROM
    source
)
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
