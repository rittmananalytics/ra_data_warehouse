{% if var("crm_warehouse_contact_sources") and var("marketing_warehouse_deal_sources")%}


WITH t_contact_deals_list AS (

  {% for source in var('marketing_warehouse_deal_sources') %}
    {% set relation_source = 'stg_' + source + '_contact_deals' %}

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
  t_contact_deals_list

{% else %}

{{
    config(
        enabled=false
    )
}}


{% endif %}
