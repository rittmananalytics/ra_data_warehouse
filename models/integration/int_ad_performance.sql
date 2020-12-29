{% if var('marketing_warehouse_ad_performance_sources') %}

with ad_performance as
  (
    {% for source in var('marketing_warehouse_ad_performance_sources') %}
      {% set relation_source = 'stg_' + source + '_ad_performance' %}

      select
        '{{source}}' as source,
        *
        from {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
select * from ad_performance

{% else %}

{{
    config(
        enabled=false
    )
}}


{% endif %}
