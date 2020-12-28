{% if not var("enable_marketing_warehouse") or not var("ad_campaigns_only") %}
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
select {{ dbt_utils.surrogate_key(['ad_id']) }} as ad_pk,
       a.*
from ads a
