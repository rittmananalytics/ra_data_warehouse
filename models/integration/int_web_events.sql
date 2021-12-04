{% if var('product_warehouse_event_sources') %}

with events_merge_list as
  (
    {% for source in var('product_warehouse_event_sources') %}

      {% set relation_source = 'stg_' + source + '_events' %}

      SELECT
        '{{source}}' AS source,
        *
        FROM {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
SELECT
  e.*,
FROM events_merge_list e


{% else %}

{{config(enabled=false)}}

{% endif %}
