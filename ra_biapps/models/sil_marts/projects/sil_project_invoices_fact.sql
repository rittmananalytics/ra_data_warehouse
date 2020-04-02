{{
    config(
        alias='project_invoices_fact'
    )
}}
with companies_dim as (
    select *
    from {{ ref('sil_companies_dim') }}
)
,
  projects_dim as (
      select *
      from {{ ref('sil_projects_dim') }}
),
  sde_invoices_fs as (
    select *
      from {{ ref('sde_invoices_fs') }}
  )
select GENERATE_UUID() as invoice_pk,
       c.company_pk,
       p.project_pk,
       i.* except (company_id)
 FROM
       sde_invoices_fs i
       JOIN companies_dim c
          ON cast(i.company_id as string) IN UNNEST(c.all_company_ids)
       LEFT OUTER JOIN projects_dim p
          ON i.harvest_project_id = p.harvest_project_id
