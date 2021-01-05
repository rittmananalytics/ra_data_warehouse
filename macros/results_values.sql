{% macro results_values(results) %}
  {% for res in results -%}
    {% if loop.index > 1 %},{% endif %}
    ('{{ res.node.alias }}', '{{ res.status }}',
      case when '{{ res.status }}' like 'CREATE TABLE%' or '{{ res.status }}' like 'MERGE%' then
      {% if target.type == 'bigquery' %}
        safe_cast(replace(split('{{ res.status }}','(')[OFFSET(1)],')','') as numeric)
      {% elif target.type == 'snowflake' %}
        try_cast(replace(split_part('{{ res.status }}','(',2),')','') as numeric)
      {% else %}
          {{ exceptions.raise_compiler_error(target.type ~" not supported in this project") }}
      {% endif %}
      else 0 end,

      {{ res.execution_time }},  {{ dbt_utils.current_timestamp() }})
  {% endfor %}
{% endmacro %}
