{% if not var("enable_facebook_ads_source") or (not var("enable_marketing_warehouse")) %}
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
select GENERATE_UUID() as campaign_pk,
       c.*
from campaigns c
