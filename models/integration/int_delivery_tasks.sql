{% if var("projects_warehouse_delivery_sources") %}

with t_tasks_merge_list as
  (
    {% for source in var('projects_warehouse_delivery_sources') %}
      {% set relation_source = 'stg_' + source + '_tasks' %}

      SELECT
        '{{source}}' AS source,
        *
        FROM {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
SELECT *

 FROM t_tasks_merge_list

{% else %} {{config(enabled=false)}} {% endif %}
