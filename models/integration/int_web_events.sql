{% if (not var("enable_segment_events_source") and not var("enable_mixpanel_events_source")) or (not var("enable_marketing_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with events_merge_list as
  (
    {% if var("enable_segment_events_source")  %}

    SELECT *
    FROM   {{ ref('stg_segment_events_pageviews') }}

    {% endif %}

    {% if var("enable_segment_events_source") and var("enable_segment_dashboard_events_source") %}
    UNION ALL
    {% endif %}

    {% if var("enable_segment_dashboard_events_source") and var("enable_segment_dashboard_tracks") %}
    SELECT *
    FROM   {{ ref('stg_segment_dashboard_events_events') }}

    {% endif %}

    {% if (var("enable_segment_events_source") or var("enable_segment_dashboard_events_source")) and var("enable_segment_switcherapi_events_source") %}
    UNION ALL
    {% endif %}

    {% if var("enable_segment_switcherapi_events_source") %}
    SELECT *
    FROM   {{ ref('stg_segment_switcherapi_events_events') }}
    {% endif %}
  )
select * from events_merge_list
