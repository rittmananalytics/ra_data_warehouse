{% if not var("enable_stripe_subscriptions_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
{% if var("etl") == 'segment' %}
  {{  profile_schema(var('segment_schema')) }}
{% elif var("etl") == 'stitch' %}
  {{  profile_schema(var('stitch_schema')) }}
{% endif %}
