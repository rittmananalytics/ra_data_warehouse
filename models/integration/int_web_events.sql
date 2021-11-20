{% if var('product_warehouse_event_sources') %}

with events_merge_list as
  (
    {% for source in var('product_warehouse_event_sources') %}

      {% set relation_source = 'stg_' + source + '_events' %}

      SELECT
        '{{source}}' AS source,
        *
        FROM {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  ),
order_conversions AS (
  SELECT
    order_id,
    user_id,
    total_revenue,
    currency_code
  FROM {{ ref('int_order_conversions') }}
)


SELECT
  e.*,
  CAST(case when e.event_type = '{{ var('attribution_conversion_event_type') }}'
    then e.event_details end
    AS {{ dbt_utils.type_string() }})
    AS order_id,
    total_revenue,
    currency_code
FROM events_merge_list e
left join order_conversions o
on CAST(case when e.event_type = '{{ var('attribution_conversion_event_type') }}'
  then e.event_details end
  AS {{ dbt_utils.type_string() }}) = o.order_id


{% else %}

{{config(enabled=false)}}

{% endif %}
