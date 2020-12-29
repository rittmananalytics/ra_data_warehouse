{% if var('marketing_warehouse_email_list_sources') %}

with t_email_lists_merge_list as
  (
    {% for source in var('marketing_warehouse_email_list_sources') %}
      {% set relation_source = 'stg_' + source + '_list_members' %}

      select
        '{{source}}' as source,
        *
        from {{ ref(relation_source) }}

        {% if not loop.last %}union all{% endif %}
      {% endfor %}
  )
select * from t_email_lists_merge_list

{% else %}

{{config(enabled=false)}}

{% endif %}
