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
)
{% endif %}
SELECT
   {{ dbt_utils.surrogate_key(['invoice_number']) }} as invoice_pk,
   c.company_pk,
   row_number() over (partition by c.company_pk order by invoice_sent_at_ts) as invoice_seq,
   {{ dbt_utils.datediff('min(date(invoice_sent_at_ts)) over (partition by c.company_pk RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)', 'invoice_sent_at_ts', 'MONTH') }}  as months_since_first_invoice,
   {{ dbt_utils.date_trunc('MONTH', 'min(date(invoice_sent_at_ts)) over (partition by c.company_pk RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)') }} first_invoice_month,
   {{ dbt_utils.date_trunc('MONTH', 'max(date(invoice_sent_at_ts)) over (partition by c.company_pk RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)') }} last_invoice_month,
   {{ dbt_utils.datediff('min(date(invoice_sent_at_ts)) over (partition by c.company_pk  RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)', 'date(invoice_sent_at_ts)', 'QUARTER') }}  as quarters_since_first_invoice,
   {{ dbt_utils.date_trunc('QUARTER','min(date(invoice_sent_at_ts)) over (partition by c.company_pk RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)') }} first_invoice_quarter,
{% if var("enable_harvest_projects_source") %}
 /*  s.user_pk as creator_users_pk, */
   p.timesheet_project_pk,
{% endif %}
   i.*
FROM
   invoices i
JOIN companies_dim c
      ON i.company_id IN UNNEST(c.all_company_ids)
{% if var("enable_harvest_projects_source") %}
/*JOIN user_dim s
   ON cast(i.invoice_creator_users_id as string) IN UNNEST(s.all_user_ids)*/
LEFT OUTER JOIN projects_dim p
   ON cast(i.project_id as string) = p.timesheet_project_id
{% endif %}
