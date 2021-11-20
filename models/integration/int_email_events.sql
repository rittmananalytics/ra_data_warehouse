{% if var('marketing_warehouse_email_event_sources') %}

{{config(materialized="table")}}

with t_email_campaign_events_merge_list as
  (
    {% for source in var('marketing_warehouse_email_event_sources') %}
      {% set relation_source = 'stg_' + source + '_events' %}

      SELECT
        '{{source}}' AS source,
        *
        FROM {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
SELECT * FROM t_email_campaign_events_merge_list

{% else %}

{{config(enabled=false)}}

{% endif %}
