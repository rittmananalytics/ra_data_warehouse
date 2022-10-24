{% macro collect_freshness(source, loaded_at_field, filter) %}
  {{ return(adapter.dispatch('collect_freshness')(source, loaded_at_field, filter))}}
{% endmacro %}


{% macro default__collect_freshness(source, loaded_at_field, filter) %}
  {% call statement('collect_freshness', fetch_result=True, auto_begin=False) -%}

  {%- set enabled_array = [] -%}
  {% for node in graph.sources.values() %}
    {% if node.name == source.name %}
      {% if (node.meta['is_enabled'] | default(true)) %}
        {%- do enabled_array.append(1) -%}
      {% endif %}
    {% endif %}
  {% endfor %}
  {% set is_enabled = (enabled_array != []) %}

    select
      {% if is_enabled %}
      max({{ loaded_at_field }})
      {% else %} 
      {{ current_timestamp() }} {% endif %} as max_loaded_at,
      {{ current_timestamp() }} as snapshotted_at

    {% if is_enabled %}
    from {{ source }}
      {% if filter %}
      where {{ filter }}
      {% endif %}
    {% endif %}

  {% endcall %}
  {{ return(load_result('collect_freshness').table) }}
{% endmacro %}