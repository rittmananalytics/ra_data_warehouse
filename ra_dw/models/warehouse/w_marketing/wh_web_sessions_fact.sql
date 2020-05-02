{% if not var("enable_segment_events_source") or (not var("enable_marketing_warehouse")) %}
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
    FROM   {{ ref('int_web_sessions') }}
  )
SELECT

    GENERATE_UUID() as web_session_pk,
    s.*
FROM
   sessions s
