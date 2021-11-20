{% if var('finance_warehouse_journal_sources') %}


with journal_merge_list as
  (
    {% for source in var('finance_warehouse_journal_sources') %}

      {% set relation_source = 'stg_' + source + '_journals' %}

      SELECT
        '{{source}}' AS source,
        *
        FROM {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
SELECT * FROM journal_merge_list

{% else %}

{{config(enabled=false)}}

{% endif %}
