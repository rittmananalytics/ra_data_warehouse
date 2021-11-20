{% if var("subscriptions_warehouse_sources")  %}


with subscriptions_merge_list as
  (
    SELECT *
    FROM   {{ ref('stg_stripe_subscriptions_subscriptions') }}
  )
SELECT * FROM subscriptions_merge_list

{% else %}

{{
    config(
        enabled=false
    )
}}


{% endif %}
