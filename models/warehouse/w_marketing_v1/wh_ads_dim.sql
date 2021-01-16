{% if var("marketing_warehouse_ad_sources")  %}

{{
    config(
        unique_key='campaign_pk',
        alias='ads_dim'
    )
}}


WITH ads AS
  (
  SELECT * from {{ ref('int_ads') }}
)
select {{ dbt_utils.surrogate_key(['ad_id']) }} as ad_pk,
       a.*
from ads a

{% else %} {{config(enabled=false)}} {% endif %}
