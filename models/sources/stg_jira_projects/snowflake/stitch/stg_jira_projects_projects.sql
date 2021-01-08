{{config(enabled = target.type == 'snowflake')}}
{% if var("projects_warehouse_delivery_sources") %}
{% if 'jira_projects' in var("projects_warehouse_delivery_sources") %}

with source as (SELECT *
  FROM
  (
  SELECT

    concat('{{ var('stg_jira_projects_id-prefix') }}',id) as project_id,
    concat('{{ var('stg_jira_projects_id-prefix') }}',replace(name,' ','_')) AS company_id,
    concat('{{ var('stg_jira_projects_id-prefix') }}',lead:accountId::STRING) as lead_user_id,
    name as project_name,
    projectkeys.value::STRING as projectkeys,
    projecttypekey as project_type_id,
    cast (null as string) as project_status,

    cast (null as string) as project_notes,

    projectcategory:id::STRING as project_category_id,
    _sdc_batched_at,
    MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
  FROM
    {{ var('stg_jira_projects_stitch_projects_table') }},table(flatten(projectkeys)) projectkeys
  )
WHERE
  _sdc_batched_at = max_sdc_batched_at),
types as (SELECT
    *
    FROM (
    SELECT
      key as project_type_id,
      formattedKey as project_type,
        _sdc_batched_at,
      MAX(_sdc_batched_at) OVER (PARTITION BY key ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
    FROM
      {{ var('stg_jira_projects_stitch_project_types_table') }})
  WHERE
    _sdc_batched_at = max_sdc_batched_at),
categories as (SELECT
      *
      FROM (
      SELECT
        id as project_category_id,
        description as project_category_description,
        name as project_category_name,
          _sdc_batched_at,
        MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
      FROM
        {{ var('stg_jira_projects_stitch_project_categories_table') }})
    WHERE
      _sdc_batched_at = max_sdc_batched_at)
select p.project_id,
       p.lead_user_id,
       p.company_id,
       p.project_name,
       p.project_status,
       p.project_notes,
       t.project_type as project_type,
       c.project_category_description,
       c.project_category_name,
       cast (null as timestamp) as project_created_at_ts,
       cast (null as timestamp) as project_modified_at_ts

from source p
left outer join types t on p.project_type_id = t.project_type_id
left outer join categories c on p.project_category_id = c.project_category_id

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
