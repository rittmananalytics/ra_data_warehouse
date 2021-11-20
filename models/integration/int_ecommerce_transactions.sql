{% if var('ecommerce_warehouse_transaction_sources') %}
{{config(materialized="table")}}


with transactions_merge_list as
  (
    {% for source in var('ecommerce_warehouse_transaction_sources') %}

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
