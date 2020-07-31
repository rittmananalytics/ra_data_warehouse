{% macro get_manifest(manifest) %}
  {% for man, model in manifest -%}
    {% if loop.index > 1 %},{% endif %}
    ('{{ model.package_name }}')
  {% endfor %}
{% endmacro %}
