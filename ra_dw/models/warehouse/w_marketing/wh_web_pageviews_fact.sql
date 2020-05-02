{% if not var("enable_segment_events_source") or (not var("enable_marketing_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        alias='web_pageviews_fact'
    )
}}
{% endif %}

with pageviews as
  (
    SELECT *
    FROM   {{ ref('int_web_pageviews') }}
  )
SELECT

    GENERATE_UUID() as web_pageview_pk,
    p.*
FROM
   pageviews p
