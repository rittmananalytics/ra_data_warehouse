{% if var('marketing_warehouse_ad_campaign_sources') %}

with ad_reporting as
  (
SELECT
  campaign_id     AS ad_campaign_id,
  campaign_name   AS ad_campaign_name,
  platform        AS ad_network,
  account_name    AS ad_account_name,
  account_id      AS ad_account_id,
  utm_source      AS utm_source,
  utm_medium      AS utm_medium,
  utm_campaign    AS utm_campaign,
  utm_content     AS utm_content,
  utm_term        AS utm_term
FROM
  {{ ref('int_ad_reporting') }}
GROUP BY
  1,2,3,4,5,6,7,8,9,10
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
