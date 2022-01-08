{% macro iff() %}

{% if target.type == 'bigquery' or target.type == 'redshift' %}
  IF
{% else %}
  IFF
{% endif %}

{% endmacro %}
