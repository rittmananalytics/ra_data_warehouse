{% if var("ecommerce_warehouse_customer_cohorts_sources")  %}
{{config(materialized="table")}}


with customer_cohorts_merge_list as
  (
    {% for source in var('ecommerce_warehouse_customer_cohorts_sources') %}
      {% set relation_source = 'stg_' + source + '_customer_cohorts' %}

      select
        '{{source}}' as source,
        *
        from {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
select * from customer_cohorts_merge_list


{% else %}

{{
    config(
        enabled=false
    )
}}


{% endif %}
