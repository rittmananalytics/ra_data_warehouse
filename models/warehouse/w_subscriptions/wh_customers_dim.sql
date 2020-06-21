{% if (not var("enable_segment_events_source") and var("enable_mixpanel_events_source")) or (not var("enable_marketing_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        alias='customers_dim'
    )
}}
{% endif %}

with customers as
  (
    SELECT *
    FROM {{ ref('int_customers') }}
  )
SELECT

    GENERATE_UUID() as customer_pk,
    c.*
FROM
   customers c
