{% if not var("enable_marketing_warehouse")  %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        unique_key='campaign_pk',
        alias='ad_campaigns_dim'
    )
}}
{% endif %}

WITH campaigns AS
  (
  SELECT * from {{ ref('int_ad_campaigns') }}
)
select {{ dbt_utils.surrogate_key(['ad_campaign_id']) }}  as ad_campaign_pk,
       c.*
from campaigns c
