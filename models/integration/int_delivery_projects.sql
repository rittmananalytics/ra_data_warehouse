{% if var("projects_warehouse_delivery_sources") %}

with t_delivery_projects_merge_list as
  (
    {% for source in var('projects_warehouse_delivery_sources') %}
      {% set relation_source = 'stg_' + source + '_projects' %}

      select
        '{{source}}' as source,
        *
        from {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
select * from t_delivery_projects_merge_list

{% else %} {{config(enabled=false)}} {% endif %}
