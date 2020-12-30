{% if var("subscriptions_warehouse_sources")  %}


with customers_merge_list as
  (
    {% if var("enable_segment_dashboard_events_source") %}

    SELECT *
    FROM   {{ ref('stg_segment_dashboard_events_customers') }}

    {% endif %}
    {% if var("enable_segment_dashboard_events_source") and var("enable_stripe_payments_source")  %}
    UNION ALL
    {% endif %}
    {% if var("enable_stripe_payments_source")  %}

    SELECT *
    FROM   {{ ref('stg_stripe_subscriptions_customers') }}

    {% endif %}
  ),
merged as (

    select customer_id,
    max(customer_email) as customer_email,
    max(customer_description) as customer_description,
    max(customer_alternative_id) as customer_alternative_id,
    max(customer_plan) as customer_plan,
    max(customer_source) as customer_source,
    max(customer_type) as customer_type,
    max(customer_industry) as customer_industry,
    max(customer_currency) as customer_currency,
    max(customer_is_enterprise) as customer_is_enterprise,
    max(customer_is_delinquent) as customer_is_delinquent,
    max(customer_is_deleted) as customer_is_deleted,
    max(customer_created_date) as customer_created_date,
    max(customer_last_modified_date) as customer_last_modified_date
  from customers_merge_list
    group by 1
)
select * from merged

{% else %}

{{
    config(
        enabled=false
    )
}}


{% endif %}
