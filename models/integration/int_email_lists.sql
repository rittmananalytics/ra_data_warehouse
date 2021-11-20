{% if var('marketing_warehouse_email_list_sources') %}

with t_email_lists_merge_list as
  (
    {% for source in var('marketing_warehouse_email_list_sources') %}
      {% set relation_source = 'stg_' + source + '_list_members' %}

      SELECT
        '{{source}}' AS source,
        *
        FROM {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
SELECT * FROM t_email_lists_merge_list

{% else %}

{{config(enabled=false)}}

{% endif %}
