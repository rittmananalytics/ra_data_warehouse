{% if not var("enable_looker_usage_source") or (not var("enable_product_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        unique_key='looker_usage_pk',
        alias='looker_usage_fact'
    )
}}
{% endif %}

WITH usage AS
  (
  SELECT * from {{ ref('int_looker_usage') }}
)
select GENERATE_UUID() as usage_pk,
       u.*
from usage u
