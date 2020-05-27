{% if (not var("enable_segment_events_source") and var("enable_mixpanel_events_source")) or (not var("enable_marketing_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        alias='web_sessions_fact'
    )
}}
{% endif %}

with sessions as
  (
    SELECT *
    FROM   {{ ref('int_web_events_sessions_stitched') }}
  )
SELECT

    GENERATE_UUID() as web_sessions_pk,
    s.*
FROM
   sessions s
