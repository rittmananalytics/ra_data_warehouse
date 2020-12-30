{% if var("subscriptions_warehouse_sources")  %}
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

   {% else %}

      {{config(enabled=false)}}

   {% endif %}
