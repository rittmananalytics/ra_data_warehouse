{% if var('ecommerce_warehouse_product_sources') %}

with t_products_merge_list as
  (
    {% for source in var('ecommerce_warehouse_product_sources') %}
      {% set relation_source = 'stg_' + source + '_products' %}

      select
        '{{source}}' as source,
        *
        from {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
select * from t_products_merge_list

{% else %}

{{config(enabled=false)}}

{% endif %}
