{% if var('finance_warehouse_transaction_sources') %}


with transactions_merge_list as
  (
    {% for source in var('finance_warehouse_transaction_sources') %}

      {% set relation_source = 'stg_' + source + '_transactions' %}

      SELECT
        '{{source}}' AS source,
        *
        FROM {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
SELECT * FROM transactions_merge_list

{% else %}

{{config(enabled=false)}}

{% endif %}
