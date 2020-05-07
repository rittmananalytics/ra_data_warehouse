{% if not var("enable_projects_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        unique_key='delivery_projects_pk',
        alias='delivery_projects_dim'
    )
}}
{% endif %}

WITH delivery_projects AS
  (
  SELECT *
  FROM   {{ ref('int_delivery_projects') }}
),
companies_dim as (
    select *
    from {{ ref('wh_companies_dim') }}
)
SELECT
   GENERATE_UUID() as delivery_project_pk,
   p.project_id,
   c.company_pk,
   p.project_name,
   p.project_status,
   p.project_notes,
   p.project_type as project_type,
   p.project_category_description,
   p.project_category_name,
   p.project_created_at_ts,
   p.project_modified_at_ts
FROM
   delivery_projects p
   JOIN companies_dim c
      ON cast(p.company_id as string) IN UNNEST(c.all_company_ids)
