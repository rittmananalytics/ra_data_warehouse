{% if not var("enable_stripe_subscriptions_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}


with subscription_invoices_merge_list as
  (
    SELECT *
    FROM   {{ ref('stg_stripe_subscriptions_invoices') }}
  )
select * from subscription_invoices_merge_list
