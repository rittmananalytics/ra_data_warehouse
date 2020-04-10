{{
    config(
        unique_key='invoice_pk',
        alias='invoices_fact'
    )
}}
WITH invoices AS
  (
  SELECT *
  FROM   {{ ref('sde_invoices_fs') }}
  ),
    user_dim as (
      select *
      from {{ ref('sil_users_dim') }}
  ),
  companies_dim as (
      select *
      from {{ ref('sil_companies_dim') }}
  ),
  projects_dim as (
      select *
      from {{ ref('sil_timesheet_projects_dim') }}
)
SELECT
   GENERATE_UUID() as invoice_pk,
   s.user_pk as creator_users_pk,
   c.company_pk,
   p.timesheet_project_pk,
   i.* except (harvest_invoice_id, xero_invoice_id),

FROM
   invoices i
JOIN user_dim s
   ON cast(i.invoice_creator_users_id as string) IN UNNEST(s.all_user_ids)
JOIN companies_dim c
   ON i.company_id = c.company_id
LEFT OUTER JOIN projects_dim p
   ON cast(i.timesheet_project_id as string) = p.timesheet_project_id
