{% if not var("enable_stripe_payments_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}


with subscriptions_merge_list as
  (
    SELECT *
    FROM   {{ ref('stg_stripe_subscriptions_subscriptions') }}
  )
select * from subscriptions_merge_list
