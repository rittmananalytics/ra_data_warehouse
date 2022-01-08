{% if  var("marketing_warehouse_ad_campaign_sources") and var("product_warehouse_event_sources") %}
{% if target.type == 'snowflake' %}


{{
    config(
      alias='ad_campaign_performance_fact'
    )
}}

WITH
  campaign_performance AS
  (
  SELECT * from {{ ref('int_ad_campaign_performance') }}
)
select
  {{ dbt_utils.surrogate_key(['platform','account_id','campaign_id','ad_group_id','campaign_ts']) }} as ad_campaign_performance_pk,
  *
from
  campaign_performance
{% else %}

{{config(enabled=false)}}

{% endif %}
{% endif %}
