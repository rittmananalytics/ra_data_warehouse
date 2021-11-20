{% if var("crm_warehouse_company_sources") %}

{{config(alias='companies_dim')}}

WITH companies_dim AS (
  SELECT
    {{ dbt_utils.surrogate_key(['company_name']) }} AS company_pk,
    *
  FROM
    {{ ref('int_companies') }} c
)
SELECT * FROM companies_dim

{% else %} {{config(enabled=false)}} {% endif %}
