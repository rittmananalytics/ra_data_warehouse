{% if var("projects_warehouse_delivery_sources") %}
{{
    config(
        unique_key='delivery_projects_pk',
        alias='delivery_projects_dim'
    )
}}


WITH delivery_projects AS
  (
  SELECT *
  FROM   {{ ref('int_delivery_projects') }}
),
{% if target.type == 'bigquery' %}
  companies_dim as (
    SELECT {{ dbt_utils.star(from=ref('wh_companies_dim')) }}
    from {{ ref('wh_companies_dim') }}
  )
{% elif target.type == 'snowflake' %}
companies_dim as (
    SELECT c.company_pk, cf.value::string as company_id
    from {{ ref('wh_companies_dim') }} c,table(flatten(c.all_company_ids)) cf
)
{% else %}
    {{ exceptions.raise_compiler_error(target.type ~" not supported in this project") }}
{% endif %}
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

{% else %}

{{config(enabled=false)}}

{% endif %}
