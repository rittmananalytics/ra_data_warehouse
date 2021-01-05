{% if var("projects_warehouse_timesheet_sources") %}

with t_timesheets_merge_list as
  (
    {% for source in var('projects_warehouse_timesheet_sources') %}
      {% set relation_source = 'stg_' + source + '_timesheets' %}

      select
        '{{source}}' as source,
        *
        from {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
select * from t_timesheets_merge_list

{% else %} {{config(enabled=false)}} {% endif %}
