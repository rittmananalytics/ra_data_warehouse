{% if var('marketing_warehouse_ad_campaign_performance_sources') %}


with ad_campaign_performance as
  (

    {% for source in var('marketing_warehouse_ad_campaign_performance_sources') %}
      {% set relation_source = 'stg_' + source + '_campaign_performance' %}

      select
        '{{source}}' as source,
        *
        from {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}


  )
select * from ad_campaign_performance

{% else %}

{{
    config(
        enabled=false
    )
}}


{% endif %}
