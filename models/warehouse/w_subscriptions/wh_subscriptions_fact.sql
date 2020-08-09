{% if not var("enable_stripe_subscriptions_source")  %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        alias='subscriptions_fact'
    )
}}
{% endif %}

WITH subscriptions AS
  (
  SELECT *
  FROM   {{ ref('int_subscriptions') }}
),
customers as (
  SELECT *
  FROM   {{ ref('wh_customers_dim') }}
  ),
plans as (
  SELECT *
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
