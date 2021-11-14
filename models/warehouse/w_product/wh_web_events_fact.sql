{% if var("product_warehouse_event_sources") %}

{{
    config(
        alias='web_events_fact'
    )
}}

with events as
  (
    SELECT {{ dbt_utils.surrogate_key(['event_id']) }} as web_event_pk,
    *
    FROM   {{ ref('int_web_events_sessionized') }}
  )
{% if var('marketing_warehouse_ad_campaign_sources') %}
  ,
ad_campaigns as (
  SELECT *
    FROM {{ ref('wh_ad_campaigns_dim')}}
),
joined as (
  SELECT
    e.*,
    c.ad_campaign_pk
  FROM
    events e
  LEFT JOIN
    ad_campaigns c
  ON e.utm_campaign = c.utm_campaign
)
SELECT
  *
FROM
  joined
{% else %}
SELECT
  *
FROM
  events
{% endif %}

{% else %}{{config(enabled=false)}}{% endif %}
