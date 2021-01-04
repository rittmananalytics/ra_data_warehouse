{% if var("product_warehouse_event_sources") %}

{{
    config(
        alias='web_events_fact'
    )
}}

with events as
  (
    SELECT *
    FROM   {{ ref('int_web_events_sessionized') }}
  )
{% if var('marketing_warehouse_ad_campaign_sources') %}
  ,
utm_campaign_mapping as
( SELECT *
  FROM {{ ref('utm_campaign_mapping')}}
),
ad_campaigns as (
  SELECT *
    FROM {{ ref('wh_ad_campaigns_dim')}}
)
{% endif %}

{% if var("subscriptions_warehouse_sources")  %}
,
customers as (
   SELECT *
    FROM   {{ ref('wh_customers_dim') }}
  ),
{% endif %}
,
events_with_prev_ts_event_type as
(
SELECT

    {{ dbt_utils.surrogate_key(['event_id']) }} as web_event_pk,
    e.*,

    lag(e.event_ts,1) over (partition by e.blended_user_id order by event_seq) as prev_event_ts,
    lag(e.event_type,1)  over (partition by e.blended_user_id order by event_seq) as prev_event_type
FROM
   events e
)
,
joined as
(
  SELECT
      e.*
      {% if var('marketing_warehouse_ad_campaign_sources') %},a.ad_campaign_pk{% endif %}
      {% if var("subscriptions_warehouse_sources")  %},c.customer_pk{% endif %}
  FROM
     events_with_prev_ts_event_type e
  {% if var("subscriptions_warehouse_sources")  %}
  LEFT OUTER JOIN customers c
     ON e.user_id = c.customer_id
  {% endif %}
  {% if var('marketing_warehouse_ad_campaign_sources') %}
  LEFT OUTER JOIN utm_campaign_mapping m
     ON e.utm_campaign = m.utm_campaign
     AND e.utm_source = m.utm_source
  LEFT OUTER JOIN ad_campaigns a
           ON m.ad_campaign_id = a.ad_campaign_id
  {% endif %}
)
select * from joined

{% else %}

{{config(enabled=false)}}

{% endif %}
