{% if var("ecommerce_warehouse_customer_cohorts_sources") %}

{{
    config(
        unique_key='customer_cohort_pk',
        alias='customer_cohorts_fact'
    )
}}


WITH customer_cohorts AS
  (
  SELECT *
  FROM
     {{ ref('int_customer_cohorts') }} o
),
  customers as
  (
    SELECT *
    FROM
       {{ ref('wh_customers_dim') }} o


  )
select    {{ dbt_utils.surrogate_key(
          ['h.customer_id','h.date_month']
        ) }} as customer_cohort_pk,
          h.*
FROM      customer_cohorts h
LEFT JOIN customers c
ON        h.customer_id = c.customer_id

{% else %} {{config(enabled=false)}} {% endif %}
