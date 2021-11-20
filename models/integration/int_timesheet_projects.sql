{% if var("projects_warehouse_timesheet_sources") %}


with t_projects_merge_list as
  (
    {% for source in var('projects_warehouse_timesheet_sources') %}
      {% set relation_source = 'stg_' + source + '_projects' %}

      SELECT
        '{{source}}' AS source,
        *
        FROM {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
SELECT * FROM t_projects_merge_list

{% else %} {{config(enabled=false)}} {% endif %}
