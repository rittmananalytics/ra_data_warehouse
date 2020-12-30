{% if var("subscriptions_warehouse_sources")  %}

{{
    config(
        alias='subscription_billing_fact'
    )
}}


WITH subscriptions AS
  (
  SELECT {{ dbt_utils.star(from=ref('wh_subscriptions_fact')) }}
  FROM   {{ ref('wh_subscriptions_fact') }}
),
customers as (
  SELECT {{ dbt_utils.star(from=ref('wh_customers_dim')) }}
  FROM   {{ ref('wh_customers_dim') }}
),
subscription_billing as (
  SELECT {{ dbt_utils.star(from=ref('int_subscription_billing')) }}
  FROM {{ ref('int_subscription_billing') }}
)
SELECT
   GENERATE_UUID() as subscription_billing_pk,
   c.customer_pk,
   s.subscription_pk,
   b.*
FROM
   subscription_billing b
JOIN
   customers c
ON b.customer_id = c.customer_alternative_id
JOIN
   subscriptions s
ON
   b.subscription_id = s.subscription_id

   {% else %}

      {{config(enabled=false)}}

   {% endif %}
