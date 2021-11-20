{% if var("marketing_warehouse_ad_campaign_sources")  %}

{{
    config(
        unique_key='campaign_pk',
        alias='ad_campaigns_dim'
    )
}}


WITH campaigns AS
  (
  SELECT * FROM {{ ref('int_ad_campaigns') }}
)
SELECT {{ dbt_utils.surrogate_key(['ad_campaign_id']) }}  AS ad_campaign_pk,
       c.*
FROM campaigns c

{% else %} {{config(enabled=false)}} {% endif %}
