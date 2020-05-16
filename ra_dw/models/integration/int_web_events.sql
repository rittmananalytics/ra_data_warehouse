{% if (not var("enable_segment_events_source") and not var("enable_mixpanel_events_source")) or (not var("enable_marketing_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
with events_merge_list as
  (
    {% if var("enable_segment_events_source") %}
    SELECT *
    FROM   {{ ref('stg_segment_events_events') }}
    UNION ALL
    SELECT *
    FROM   {{ ref('stg_segment_events_pageviews') }}
    {% endif %}

    {% if var("enable_segment_events_source") and var("enable_mixpanel_events_source") %}
    UNION ALL
    {% endif %}

    {% if var("enable_mixpanel_events_source") %}
    SELECT *
    FROM   {{ ref('stg_mixpanel_events_events') }}
    UNION ALL
    SELECT *
    FROM   {{ ref('stg_mixpanel_events_pageviews') }}
    {% endif %}
  )
select * from events_merge_list
