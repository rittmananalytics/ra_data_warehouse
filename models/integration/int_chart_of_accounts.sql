{% if var('finance_warehouse_payment_sources') %}


with t_chart_of_accounts_merge_list as
  (
    {% for source in var('finance_warehouse_payment_sources') %}
      {% set relation_source = 'stg_' + source + '_accounts' %}

      select
        '{{source}}' as source,
        *
        from {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
SELECT * from t_chart_of_accounts_merge_list

{% else %} {{config(enabled=false)}} {% endif %}
