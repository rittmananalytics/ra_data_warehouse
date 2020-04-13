{% if not var("enable_crm_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        alias='deals_fact'
    )
}}
{% endif %}

with companies_dim as (
    select *
    from {{ ref('sil_companies_dim') }}
),
  user_dim as (
    select user_pk, all_user_emails
    from {{ ref('sil_users_dim') }}
  )
SELECT
   GENERATE_UUID() as deal_pk,
   c.company_pk,
   d.* except (company_id,deal_assigned_consultant,deal_salesperson_email),
   s.user_pk as deal_assigned_consultant_users_pk,
   sp.user_pk as deal_salesperson_users_pk
FROM
   {{ ref('sde_deals_fs') }} d
JOIN companies_dim c
   ON d.company_id IN UNNEST(c.all_company_ids)
JOIN user_dim s
   ON d.deal_assigned_consultant in UNNEST(s.all_user_emails)
JOIN user_dim sp
   ON d.deal_salesperson_email IN UNNEST(sp.all_user_emails)
