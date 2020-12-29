{% if var("marketing_warehouse_deal_sources") and var("crm_warehouse_company_sources") %}

{{
    config(
        alias='deals_fact',
        unique_key='deal_id'
    )
}}


with companies_dim as (
    select *
    from {{ ref('wh_companies_dim') }}
)
SELECT
    {{ dbt_utils.surrogate_key(['deal_id']) }} as deal_pk,
     c.company_pk,
     d.* except (company_id)
FROM
   {{ ref('int_deals') }} d
JOIN companies_dim c
   ON d.company_id IN UNNEST(c.all_company_ids)

{% else %} {{config(enabled=false)}} {% endif %}
