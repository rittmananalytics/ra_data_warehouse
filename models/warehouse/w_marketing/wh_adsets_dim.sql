{% if not var("enable_facebook_ads_source") or (not var("enable_marketing_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        unique_key='adset_pk',
        alias='adsets_dim'
    )
}}
{% endif %}

WITH adsets AS
  (
  SELECT * from {{ ref('int_adsets') }}
)
select GENERATE_UUID() as adset_pk,
       a.*
from adsets a
