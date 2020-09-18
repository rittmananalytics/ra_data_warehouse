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
    UNION ALL
    SELECT *
    FROM   {{ ref('stg_segment_events_events') }}

    {% endif %}

    {% if var("enable_segment_events_source") and var("enable_mixpanel_events_source") %}
    UNION ALL
    {% endif %}

    {% if var("enable_mixpanel_events_source") %}
    SELECT *
    FROM   {{ ref('stg_mixpanel_events_events') }}

    {% endif %}
  )


select
  e.*

{% if var("enable_event_type_mapping")   %}
  except (event_type),
  coalesce(m.event_type_mapped,e.event_type) as event_type
{% endif %}

from events_merge_list e

{% if var("enable_event_type_mapping")   %}
left outer join
  {{ ref('event_mapping_list') }} m
on
  e.event_type = m.event_type_original
{% endif %}
