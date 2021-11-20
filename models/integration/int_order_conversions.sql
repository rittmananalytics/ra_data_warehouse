{% if var('order_conversion_sources') %}

WITH t_order_conversions AS (

  {% for source in var('order_conversion_sources') %}
    {% set relation_source = 'stg_' + source + '_order_conversions' %}

    SELECT
      '{{source}}' AS source,
      *
      FROM {{ ref(relation_source) }}

      {% if not loop.last %}union all{% endif %}
    {% endfor %}
)
SELECT
  *
FROM
  t_order_conversions

{% else %} {{config(enabled=false)}} {% endif %}
