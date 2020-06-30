{% if not var("enable_subscriptions_warehouse")  %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        alias='plans_dim'
    )
}}
{% endif %}

with plans as
  (
    SELECT *
    FROM {{ ref('int_plans') }}
  )
SELECT

    GENERATE_UUID() as plan_pk,
    p.*
FROM
   plans p
