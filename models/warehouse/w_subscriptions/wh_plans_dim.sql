{% if var("subscriptions_warehouse_sources")  %}

{{
    config(
        alias='plans_dim'
    )
}}


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
