{% if var("subscriptions_warehouse_sources")  %}


with subscription_invoices_merge_list as
  (
    SELECT *
    FROM   {{ ref('stg_stripe_subscriptions_invoices') }}
  )
select * from subscription_invoices_merge_list

{% else %}

{{
    config(
        enabled=false
    )
}}


{% endif %}
