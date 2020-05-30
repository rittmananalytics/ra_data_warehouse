{% if not var("enable_segment_events_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
{% if var("etl") == 'segment' %}
  {{  profile_schema(var('segment_schema')) }}
{% endif %}
