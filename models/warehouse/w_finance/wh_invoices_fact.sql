{% if not var("enable_finance_warehouse") and not var("enable_projects_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        unique_key='invoice_pk',
        alias='invoices_fact'
    )
}}
{% endif %}

WITH invoices AS
  (
  SELECT *
  FROM   {{ ref('int_invoices') }}
  ),
  companies_dim as (
      select *
      from {{ ref('wh_companies_dim') }}
  )
{% if var("enable_harvest_projects_source") %},
  projects_dim as (
      select *
      from {{ ref('wh_timesheet_projects_dim') }}
),
  user_dim as (
    select *
    from {{ ref('wh_users_dim') }}
)
{% endif %}
SELECT
   GENERATE_UUID() as invoice_pk,
   c.company_pk,
   row_number() over (partition by c.company_pk order by invoice_sent_at_ts) as invoice_seq,
   date_diff(date(invoice_sent_at_ts),min(date(invoice_sent_at_ts)) over (partition by c.company_pk),MONTH) as months_since_first_invoice,
   timestamp(date_trunc(min(date(invoice_sent_at_ts)) over (partition by c.company_pk),MONTH)) first_invoice_month,
   date_diff(date(invoice_sent_at_ts),min(date(invoice_sent_at_ts)) over (partition by c.company_pk),QUARTER) as quarters_since_first_invoice,
   timestamp(date_trunc(min(date(invoice_sent_at_ts)) over (partition by c.company_pk),QUARTER)) first_invoice_quarter,
{% if var("enable_harvest_projects_source") %}
   s.user_pk as creator_users_pk,
   p.timesheet_project_pk,
{% endif %}
   i.*
FROM
   invoices i
JOIN companies_dim c
      ON i.company_id IN UNNEST(c.all_company_ids)
{% if var("enable_harvest_projects_source") %}
JOIN user_dim s
   ON cast(i.invoice_creator_users_id as string) IN UNNEST(s.all_user_ids)
JOIN projects_dim p
   ON cast(i.project_id as string) = p.timesheet_project_id
{% endif %}
