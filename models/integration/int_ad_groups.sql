{% if var('marketing_warehouse_ad_group_sources') %}

with ad_reporting as
  (
    SELECT
      ad_group_id   as ad_group_id,
      ad_group_name as ad_group_name,
      campaign_id   as ad_campaign_id,
      campaign_name as ad_campaign_name,
      platform      as ad_network,
      account_name  as ad_account_name,
      account_id    as ad_account_id,
      base_url      as ad_base_url,
      url_host      as ad_url_host,
      url_path      as ad_url_path,
      utm_source    as ad_utm_source,
      utm_medium    as ad_utm_medium,
      utm_campaign  as ad_utm_campaign,
      utm_content   as ad_utm_content,
      utm_term      as ad_utm_term
FROM
  {{ ref('int_ad_reporting') }}
GROUP BY
  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)
select *
from ad_reporting
{% else %}

{{
    config(
        enabled=false
    )
}}


{% endif %}
