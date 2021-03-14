{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contact_sources") and var("crm_warehouse_company_sources") %}
{{config(alias='contacts_segments_xa',
         materialized="view")}}
SELECT
  email,
  name,
  case when visits_l90_days > 30 then 'Highly Engaged'
       when visits_l90_days between 1 and 30 then 'Engaged'
       else 'Historic' end as engagement_level,
  case when pricing_views > 0 then 'Buying' else 'Researching' end as buying_stage,
  CASE
    WHEN influencer_status IS NULL AND (pricing_views >0 OR casestudy_views > 0) THEN 'Prospect'
    WHEN influencer_status IS NULL AND(pricing_views =0
    AND casestudy_views =0) THEN 'Visitor'
    WHEN influencer_status = 'Influencer' AND (attribution_interest+casestudy_views+customer_journey_interest+data_centralisation_interest+data_teams_interest+dbt_interest+looker_interest+personas_interest+ra_warehouse_interest+ segment_interest)>0 THEN 'Engaged Influencer'
    WHEN influencer_status = 'Champion'
  AND (pricing_views >0
    OR casestudy_views > 0) THEN 'Champion Prospect'
  ELSE
  'Contact'
END
  AS contact_segment
FROM
  {{ ref('wh_contacts_audiences_xa') }}
  {% else %} {{config(enabled=false)}} {% endif %}
