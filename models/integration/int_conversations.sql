{% if var('crm_warehouse_conversations_sources') %}



with conversations_merge_list as
  (
    {% for source in var('crm_warehouse_conversations_sources') %}
      {% set relation_source = 'stg_' + source + '_conversations' %}

      SELECT
        '{{source}}' AS source,
        *
        FROM {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
SELECT * FROM conversations_merge_list

{% else %} {{config(enabled=false)}} {% endif %}
