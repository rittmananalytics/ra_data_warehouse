{% if var("marketing_warehouse_deal_sources") %}


with t_deals_merge_list as
  (
    {% for source in var('marketing_warehouse_deal_sources') %}
      {% set relation_source = 'stg_' + source + '_deals' %}

      select
        '{{source}}' as source,
        *
        from {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
select * from t_deals_merge_list

{% else %}

{{
    config(
        enabled=false
    )
}}


{% endif %}
