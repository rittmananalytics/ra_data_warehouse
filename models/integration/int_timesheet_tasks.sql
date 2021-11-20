{% if var("projects_warehouse_timesheet_sources") %}

with t_tasks_merge_list as
  (
    {% for source in var('projects_warehouse_timesheet_sources') %}
      {% set relation_source = 'stg_' + source + '_tasks' %}

      SELECT
        '{{source}}' AS source,
        *
        FROM {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
SELECT * FROM t_tasks_merge_list
union all
SELECT 'unknown_values' AS source,
* FROM {{ ref('stg_unknown_projects_tasks') }}

{% else %} {{config(enabled=false)}} {% endif %}
