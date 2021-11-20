{% if var('marketing_warehouse_ad_group_sources') %}

with ad_reporting as
  (
    SELECT
      ad_group_id   AS ad_group_id,
      ad_group_name AS ad_group_name,
      campaign_id   AS ad_campaign_id,
      campaign_name AS ad_campaign_name,
      platform      AS ad_network,
      account_name  AS ad_account_name,
      account_id    AS ad_account_id,
      base_url      AS ad_base_url,
      url_host      AS ad_url_host,
      url_path      AS ad_url_path,
      utm_source    AS ad_utm_source,
      utm_medium    AS ad_utm_medium,
      utm_campaign  AS ad_utm_campaign,
      utm_content   AS ad_utm_content,
      utm_term      AS ad_utm_term
FROM
  {{ ref('int_ad_reporting') }}
GROUP BY
  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)
SELECT *
FROM ad_reporting
{% else %}

{{
    config(
        enabled=false
    )
}}


{% endif %}
