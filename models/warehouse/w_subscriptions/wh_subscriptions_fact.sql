{% if var("subscriptions_warehouse_sources")  %}
{{
    config(
        alias='subscriptions_fact'
    )
}}


WITH subscriptions AS
  (
  SELECT {{ dbt_utils.star(from=ref('int_subscriptions')) }}
  FROM   {{ ref('int_subscriptions') }}
),
customers as (
  SELECT {{ dbt_utils.star(from=ref('wh_customers_dim')) }}
  FROM   {{ ref('wh_customers_dim') }}
  ),
plans as (
  SELECT {{ dbt_utils.star(from=ref('wh_plans_dim')) }}
  FROM   {{ ref('wh_plans_dim') }}
  )
SELECT
   GENERATE_UUID() as subscription_pk,
   c.customer_pk,
   p.plan_pk,
   s.*
FROM
   subscriptions s
JOIN
   customers c
ON s.customer_id = c.customer_alternative_id
JOIN
   plans p
ON
   s.plan_id = p.plan_id

{% else %}

   {{config(enabled=false)}}

{% endif %}
