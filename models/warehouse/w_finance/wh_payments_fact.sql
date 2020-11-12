{% if not var("enable_finance_warehouse") and not var("enable_projects_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        unique_key='payment_pk',
        alias='payments_fact'
    )
}}
{% endif %}

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
   GENERATE_UUID() as payment_pk,
   c.company_pk,
   row_number() over (partition by c.company_pk order by payment_date) as payment_seq,
   date_diff(date(payment_date),min(date(payment_date)) over (partition by c.company_pk),MONTH) as months_since_first_payment,
   timestamp(date_trunc(min(date(payment_date)) over (partition by c.company_pk),MONTH)) first_payment_month,
   date_diff(date(payment_date),min(date(payment_date)) over (partition by c.company_pk),QUARTER) as quarters_since_first_payment,
   timestamp(date_trunc(min(date(payment_date)) over (partition by c.company_pk),QUARTER)) first_payment_quarter,
   p.*
FROM
   payments p
JOIN companies_dim c
      ON p.company_id IN UNNEST(c.all_company_ids)
JOIN currencies_dim d
      ON p.currency_code = d.currency_code
