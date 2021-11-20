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
merged AS (

    SELECT customer_id,
    max(customer_email) AS customer_email,
    max(customer_description) AS customer_description,
    max(customer_alternative_id) AS customer_alternative_id,
    max(customer_plan) AS customer_plan,
    max(customer_source) AS customer_source,
    max(customer_type) AS customer_type,
    max(customer_industry) AS customer_industry,
    max(customer_currency) AS customer_currency,
    max(customer_is_enterprise) AS customer_is_enterprise,
    max(customer_is_delinquent) AS customer_is_delinquent,
    max(customer_is_deleted) AS customer_is_deleted,
    max(customer_created_date) AS customer_created_date,
    max(customer_last_modified_date) AS customer_last_modified_date
  FROM customers_merge_list
    group by 1
)
SELECT * FROM merged

{% else %}

{{
    config(
        enabled=false
    )
}}


{% endif %}
