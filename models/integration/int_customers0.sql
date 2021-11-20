{% if var('ecommerce_warehouse_customer_sources') %}
{{config(materialized="table")}}


with customers_merge_list as
  (
    {% for source in var('ecommerce_warehouse_customer_sources') %}
      {% set relation_source = 'stg_' + source + '_customers' %}

      SELECT
        '{{source}}' AS source,
        *
        FROM {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
SELECT
  *,
  coalesce((REGEXP_COUNT ( customer_tags, 'PRO,')>0
  or REGEXP_COUNT ( customer_tags, 'PRO_25,')>0
  or REGEXP_COUNT ( customer_tags, 'PRO_20,')>0
  or REGEXP_COUNT ( customer_tags, 'PRO_15,')>0
  or REGEXP_COUNT ( customer_tags, 'PRO_FACEBOOK')),false) AS contact_is_pro
  FROM customers_merge_list


{% else %}


{{
    config(
        enabled=false
    )
}}


{% endif %}
