{{
    config(
        alias='deals_fact'
    )
}}
with companies_dim as (
    select *
    from {{ ref('sil_companies_dim') }}
),
  staff_dim as (
    select *
    from {{ ref('sil_staff_dim') }}
  )
SELECT
   GENERATE_UUID() as deal_pk,
   c.company_pk,
   d.* except (company_id,deal_assigned_consultant,deal_salesperson_email),
   s.staff_pk as deal_assigned_consultant_staff_pk,
   sp.staff_pk as deal_salesperson_staff_pk
FROM
   {{ ref('sde_deals_fs') }} d
JOIN companies_dim c
   ON d.company_id IN UNNEST(c.all_company_ids)
LEFT OUTER JOIN staff_dim s
   ON d.deal_assigned_consultant = s.staff_full_name
LEFT OUTER JOIN staff_dim sp
   ON d.deal_salesperson_email = sp.staff_email
