{% if var("projects_warehouse_timesheet_sources") %}

with t_tasks_merge_list as
  (
    {% for source in var('projects_warehouse_timesheet_sources') %}
      {% set relation_source = 'stg_' + source + '_tasks' %}

      select
        '{{source}}' as source,
        *
        from {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
select * from t_tasks_merge_list
union all
select 'unknown_values' as source,
* from {{ ref('stg_unknown_projects_tasks') }}

{% else %} {{config(enabled=false)}} {% endif %}
