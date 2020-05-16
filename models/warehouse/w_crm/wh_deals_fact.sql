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
)
SELECT
   GENERATE_UUID() as deal_pk,
   c.company_pk,
   d.* except (company_id)
FROM
   {{ ref('int_deals') }} d
JOIN companies_dim c
   ON d.company_id IN UNNEST(c.all_company_ids)
