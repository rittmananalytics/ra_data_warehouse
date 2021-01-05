{% if var("finance_warehouse_payment_sources") %}

{{
    config(
        unique_key='payment_pk',
        alias='payments_fact'
    )
}}


WITH payments AS
  (
  SELECT *
  FROM   {{ ref('int_payments') }}
  ),
  companies_dim as (
      select *
      from {{ ref('wh_companies_dim') }}
  ),
  currencies_dim as (
    select *
    from {{ ref('wh_currencies_dim') }}
)
SELECT
   {{ dbt_utils.surrogate_key(['payment_id']) }} as payment_pk,
   c.company_pk,
   row_number() over (partition by c.company_pk order by payment_date) as payment_seq,
   {{ dbt_utils.datediff('min(date(payment_date)) over (partition by c.company_pk)', 'date(payment_date)', 'MONTH') }}  as months_since_first_payment,
   {{ dbt_utils.date_trunc('MONTH','min(date(payment_date)) over (partition by c.company_pk)') }} as first_payment_month,
   {{ dbt_utils.datediff('min(date(payment_date)) over (partition by c.company_pk)', 'date(payment_date)', 'QUARTER') }}  as quarters_since_first_payment,
   {{ dbt_utils.date_trunc('QUARTER','min(date(payment_date)) over (partition by c.company_pk)') }} as first_payment_quarter,
   p.*
FROM
   payments p
JOIN companies_dim c
      ON p.company_id IN UNNEST(c.all_company_ids)
JOIN currencies_dim d
      ON p.currency_code = d.currency_code

{% else %} {{config(enabled=false)}} {% endif %}
