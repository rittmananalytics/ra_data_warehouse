{% if not var("enable_finance_warehouse") and not enable_projects_warehouse %}
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
  FROM   {{ ref('sde_invoices_fs') }}
  ),
  companies_dim as (
      select *
      from {{ ref('sil_companies_dim') }}
  )
    {% if var("enable_harvest_projects_source") %},
  projects_dim as (
      select *
      from {{ ref('sil_timesheet_projects_dim') }}
),
  user_dim as (
    select *
    from {{ ref('sil_users_dim') }}
)   {% endif %}
SELECT
   GENERATE_UUID() as invoice_pk,
   c.company_pk,
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
