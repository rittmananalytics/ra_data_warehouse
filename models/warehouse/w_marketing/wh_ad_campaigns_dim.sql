{% if var("marketing_warehouse_ad_campaign_sources")  %}

{{
    config(
        unique_key='campaign_pk',
        alias='ad_campaigns_dim'
    )
}}


WITH campaigns AS
  (
  SELECT * from {{ ref('int_ad_campaigns') }}
)
select {{ dbt_utils.surrogate_key(['ad_campaign_id']) }}  as ad_campaign_pk,
       c.*
from campaigns c

{% else %} {{config(enabled=false)}} {% endif %}
