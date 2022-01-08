{% if target.type == 'bigquery'  %}
{% if var("product_warehouse_event_sources") %}
{% if 'ga4_events_all' in var("product_warehouse_event_sources") %}

WITH
  source AS (
  SELECT
    event_bundle_sequence_id as event_id,
    event_name as event_type,
    TIMESTAMP_MICROS(MIN(event_timestamp)) as event_ts,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE event_name = 'page_view' AND key = 'page_title') AS event_details,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE event_name = 'page_view' AND key = 'page_title') AS page_title,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_url_path,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_referrer') AS referrer_host,
    cast(null as string) as search,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_url,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_url_host,
    cast(null as string) as gclid,
    traffic_source.name AS channel,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'term') AS term,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'content') AS content,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium') AS medium,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign') AS campaign,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source') AS source,
    cast(null as string) as ip,
    user_pseudo_id AS visitor_id,
    user_id as user_id,
    device.mobile_model_name	as device,
    device.web_info.hostname	as site,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') AS session_seq,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
    'GA4' as source,
    platform as platform,
    geo.country as ip_country,
    geo.region	                        AS ip_region,
    geo.city	                      AS ip_city,
    cast(null as string)	                      AS ip_zipcode,
    cast(null as string)                       AS ip_latitude,
    cast(null as string)	                        AS ip_longitude,
    geo.sub_continent		                      AS ip_region_name,
    cast(null as string)	                      AS ip_isp,
    cast(null as string)	                      AS ip_organization,
    cast(null as string)                        AS ip_domain
 FROM
    `ra-development.analytics_277223877.events_*`
    )
  select * from source

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
