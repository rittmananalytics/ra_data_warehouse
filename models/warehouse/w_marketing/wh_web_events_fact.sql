{% if (not var("enable_segment_events_source") and var("enable_mixpanel_events_source")) or (not var("enable_marketing_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        alias='web_events_fact'
    )
}}
{% endif %}

with events as
  (
    SELECT *
    FROM   {{ ref('int_web_events_sessionized') }}
  ),
pages AS
    (
    SELECT * from {{ ref('wh_web_pages_dim') }}
  )
SELECT

    GENERATE_UUID() as web_event_pk,
    p.web_page_pk,
    e.* except (page_url_host,page_title,page_url_path )
FROM
   events e
left outer join pages p
on e.page_url_host = p.page_url_host
and e.page_title = p.page_title
and e.page_url_path = p.page_url_path
