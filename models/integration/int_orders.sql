{% if var('ecommerce_warehouse_order_sources') %}
{{config(materialized="table")}}

with t_orders_merge_list as
  (
    {% for source in var('ecommerce_warehouse_order_sources') %}
      {% set relation_source = 'stg_' + source + '_orders' %}

      select
        '{{source}}' as source,
        *
        from {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
select * from t_orders_merge_list

{% else %}

{{config(enabled=false)}}

{% endif %}
