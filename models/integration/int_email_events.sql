{% if var('marketing_warehouse_email_event_sources') %}

with t_email_campaign_events_merge_list as
  (
    {% for source in var('marketing_warehouse_email_event_sources') %}
      {% set relation_source = 'stg_' + source + '_events' %}

      select
        '{{source}}' as source,
        *
        from {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
select * from t_email_campaign_events_merge_list

{% else %}

{{config(enabled=false)}}

{% endif %}
