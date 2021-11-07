{% if var("crm_warehouse_company_sources") %}

{{config(alias='companies_dim')}}

WITH companies_dim as (
  SELECT
    {{ dbt_utils.surrogate_key(['company_name']) }} as company_pk,
    *
  FROM
    {{ ref('int_companies') }} c
)
select * from companies_dim

{% else %} {{config(enabled=false)}} {% endif %}
