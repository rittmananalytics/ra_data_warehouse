{% if not var("enable_stripe_subscriptions_source")  %}
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

   {% else %}

      {{config(enabled=false)}}

   {% endif %}
