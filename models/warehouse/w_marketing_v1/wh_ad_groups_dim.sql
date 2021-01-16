{% if var("marketing_warehouse_ad_group_sources")  %}

{{
    config(
        unique_key='campaign_pk',
        alias='ad_groups_dim'
    )
}}

WITH ad_groups AS
  (
  SELECT * from {{ ref('int_ad_ad_groups') }}
)
select {{ dbt_utils.surrogate_key(['ad_group_id']) }}  as ad_group_pk,
       a.*
from ad_groups a

{% else %} {{config(enabled=false)}} {% endif %}
