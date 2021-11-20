{% if var("marketing_warehouse_ad_group_sources")  %}

{{
    config(
        unique_key='ad_group_pk',
        alias='ad_groups_dim'
    )
}}

WITH ad_groups AS
  (
  SELECT * FROM {{ ref('int_ad_groups') }}
)
SELECT {{ dbt_utils.surrogate_key(['ad_group_id']) }}  AS ad_group_pk,
       a.*
FROM ad_groups a

{% else %} {{config(enabled=false)}} {% endif %}
