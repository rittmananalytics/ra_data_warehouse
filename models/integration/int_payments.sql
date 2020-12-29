{% if var('finance_warehouse_payment_sources') %}

WITH payments_merge_list AS (

  {% for source in var('finance_warehouse_payment_sources') %}
    {% set relation_source = 'stg_' + source + '_payments' %}

    select
      '{{source}}' as source,
      *
      from {{ ref(relation_source) }}

      {% if not loop.last %}union all{% endif %}
    {% endfor %}

)
SELECT
  *
FROM
  payments_merge_list

{% else %}

  {{config(enabled=false)}}

{% endif %}
