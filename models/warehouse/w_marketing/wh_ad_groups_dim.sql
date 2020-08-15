{% if not var("enable_marketing_warehouse") or not var("ad_campaigns_only") %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        unique_key='ad_group_pk',
        alias='ad_groups_dim'
    )
}}
{% endif %}

WITH ad_groups AS
  (
  SELECT * from {{ ref('int_ad_ad_groups') }}
)
select GENERATE_UUID() as ad_group_pk,
       a.*
from ad_groups a
