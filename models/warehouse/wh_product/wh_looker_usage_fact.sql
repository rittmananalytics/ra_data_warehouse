{% if not var("enable_looker_usage_source") or (not var("enable_product_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        unique_key='looker_usage_pk',
        alias='looker_usage_fact'
    )
}}
{% endif %}

with companies_dim as (
    select *
    from {{ ref('wh_companies_dim') }}
),
usage AS
  (
  SELECT * from {{ ref('int_looker_usage') }}
)
select GENERATE_UUID() as usage_pk,
       c.company_pk,
       u.* except (company_id)
from usage u
JOIN companies_dim c
   ON cast(u.company_id as string) IN UNNEST(c.all_company_ids)
