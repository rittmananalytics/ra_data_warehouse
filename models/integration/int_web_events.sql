{% if var('product_warehouse_event_sources') %}

with events_merge_list as
  (
    {% for source in var('product_warehouse_event_sources') %}

      {% set relation_source = 'stg_' + source + '_events' %}

      select
        '{{source}}' as source,
        *
        from {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  ),
order_conversions as (
  select
    order_id,
    user_id,
    total_revenue,
    currency_code
  from {{ ref('int_order_conversions') }}
)


select
  e.*,
  cast(case when e.event_type = '{{ var('attribution_conversion_event_type') }}'
    then e.event_details end
    as {{ dbt_utils.type_string() }})
    as order_id,
    total_revenue,
    currency_code
from events_merge_list e
left join order_conversions o
on cast(case when e.event_type = '{{ var('attribution_conversion_event_type') }}'
  then e.event_details end
  as {{ dbt_utils.type_string() }}) = o.order_id


{% else %}

{{config(enabled=false)}}

{% endif %}
