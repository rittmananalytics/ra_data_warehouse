{% if not var("enable_segment_events_source") or (not var("enable_marketing_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with pageviews as
  (
    SELECT *
    FROM   {{ ref('stg_segment_events_web_page_views_sessionized') }}
  )
select * from pageviews
