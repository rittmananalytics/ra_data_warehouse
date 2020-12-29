{% if var('marketing_warehouse_ad_group_sources') %}

with ad_groups as
  (
    {% for source in var('marketing_warehouse_ad_group_sources') %}
      {% set relation_source = 'stg_' + source + '_ad_groups' %}

      select
        '{{source}}' as source,
        *
        from {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
select * from ad_groups

{% else %}

{{
    config(
        enabled=false
    )
}}


{% endif %}
