{% macro results_values(results) %}
  {% for res in results -%}
    {% if loop.index > 1 %},{% endif %}
    ('{{ res.node.alias }}', '{{ res.status }}',
      case when '{{ res.status }}' like 'CREATE TABLE%' or '{{ res.status }}' like 'MERGE%' then
      safe_cast(replace(split('{{ res.status }}','(')[OFFSET(1)],')','') as numeric)
      else 0 end,
      {{ res.execution_time }},  current_timestamp())
  {% endfor %}
{% endmacro %}
