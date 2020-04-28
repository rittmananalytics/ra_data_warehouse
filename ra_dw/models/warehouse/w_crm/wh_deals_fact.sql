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
    from {{ ref('wh_companies_dim') }}
),
  user_dim as (
    select user_pk, user_name
    from {{ ref('wh_users_dim') }}
  )
SELECT
   GENERATE_UUID() as deal_pk,
   c.company_pk,
   d.* except (company_id,deal_assigned_consultant,deal_salesperson_email),
   s.user_pk as deal_assigned_consultant_users_pk,
   sp.user_pk as deal_salesperson_users_pk
FROM
   {{ ref('int_deals') }} d
JOIN companies_dim c
   ON d.company_id IN UNNEST(c.all_company_ids)
LEFT OUTER JOIN user_dim s
  ON coalesce(d.deal_assigned_consultant,'Unassigned') = s.user_name
LEFT OUTER JOIN user_dim sp
  ON coalesce(d.deal_salesperson_email,'Unassigned') = sp.user_name
