{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("projects_warehouse_delivery_sources") %}
{% if 'jira_projects' in var("projects_warehouse_delivery_sources") %}

with source AS (SELECT
  * except (projectkeys)
  FROM (
  SELECT
    CONCAT('{{ var('stg_jira_projects_id-prefix') }}',id) AS project_id,
    CONCAT('{{ var('stg_jira_projects_id-prefix') }}',replace(name,' ','_')) AS company_id,
    CONCAT('{{ var('stg_jira_projects_id-prefix') }}',lead.accountid) AS lead_user_id,
    name AS project_name,
    projectkeys ,
    projecttypekey AS project_type_id,
    CAST(null AS {{ dbt_utils.type_string() }}) AS project_status,
    CAST(null AS {{ dbt_utils.type_string() }}) AS project_notes,

    projectcategory.id AS project_category_id,
    _sdc_batched_at,
    MAX(_sdc_batched_at) OVER (PARTITION BYid ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
  FROM
    source('stitch_jira_projects','projects')
  ),
    unnest(projectkeys) jira_project_key
WHERE
  _sdc_batched_at = max_sdc_batched_at),
types AS (SELECT
    *
    FROM (
    SELECT
      key AS project_type_id,
      formattedKey AS project_type,
        _sdc_batched_at,
      MAX(_sdc_batched_at) OVER (PARTITION BYkey ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
    FROM
      {{ source('stitch_jira_projects','project_types') }})
  WHERE
    _sdc_batched_at = max_sdc_batched_at),
categories AS (SELECT
      *
      FROM (
      SELECT
        id AS project_category_id,
        description AS project_category_description,
        name AS project_category_name,
          _sdc_batched_at,
        MAX(_sdc_batched_at) OVER (PARTITION BYid ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
      FROM
        {{ source('stitch_jira_projects','project_categories') }})
    WHERE
      _sdc_batched_at = max_sdc_batched_at)
SELECT p.project_id,
       p.lead_user_id,
       p.company_id,
       p.project_name,
       p.project_status,
       p.project_notes,
       t.project_type AS project_type,
       c.project_category_description,
       c.project_category_name,
       CAST(null AS timestamp) AS project_created_at_ts,
       CAST(null AS timestamp) AS project_modified_at_ts

FROM source p
left outer join types t on p.project_type_id = t.project_type_id
left outer join categories c on p.project_category_id = c.project_category_id

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
