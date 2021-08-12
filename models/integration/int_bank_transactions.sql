{% if var('finance_warehouse_bank_transaction_sources') %}


with bank_transactions_merge_list as
  (
    {% for source in var('finance_warehouse_bank_transaction_sources') %}

      {% set relation_source = 'stg_' + source + '_bank_transactions' %}

      select
        '{{source}}' as source,
        *
        from {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
select * from bank_transactions_merge_list

{% else %}

{{config(enabled=false)}}

{% endif %}
