{% if not var("enable_facebook_ads_source") or (not var("enable_marketing_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        unique_key='ad_pk',
        alias='ads_dim'
    )
}}
{% endif %}

WITH ads AS
  (
  SELECT * from {{ ref('int_ads') }}
)
select GENERATE_UUID() as ad_pk,
       a.*
from ads a
