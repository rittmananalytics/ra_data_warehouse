{% if not var("enable_stripe_payments_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
{% if var("etl") == 'fivetran' %}
  {{  profile_schema(var('fivetran_schema')) }}
{% elif var("etl") == 'stitch' %}
  {{  profile_schema(var('stitch_schema')) }}
{% endif %}
