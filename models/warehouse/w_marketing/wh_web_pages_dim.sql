{% if (not var("enable_segment_events_source") and var("enable_mixpanel_events_source")) or (not var("enable_marketing_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        alias='web_pages_dim'
    )
}}
{% endif %}

with pages as
  (
    SELECT page_url_host,
           page_title,
           page_url_path
    FROM {{ ref('int_web_events_sessionized') }}
    group by 1,2,3
  )
SELECT

    GENERATE_UUID() as web_page_pk,
    p.*
FROM
   pages p
