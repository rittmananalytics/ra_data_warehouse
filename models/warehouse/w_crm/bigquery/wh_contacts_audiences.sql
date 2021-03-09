{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contact_sources") and var("crm_warehouse_company_sources") %}
{{config(alias='contacts_audiences_xa',
         materialized="view")}}
WITH contacts_dim AS (SELECT
  ct.*,
  hb.contact_id as hubspot_contact_id,
  ce.contact_email as contact_email,

  c.company_pk
FROM (
  SELECT
    *
  FROM
    {{ ref('wh_contacts_dim') }},
    UNNEST( all_contact_company_ids) AS company_id  ) ct
JOIN (
  SELECT
    *
  FROM
    {{ ref('wh_companies_dim') }} c,
    UNNEST (all_company_ids) AS company_id ) c
ON
  ct.company_id = c.company_id
LEFT JOIN
  (SELECT
    contact_pk,
    contact_id
   FROM {{ ref('wh_contacts_dim') }},
   UNNEST( all_contact_ids) as contact_id
   WHERE
    contact_id like '%hubspot%' ) hb
ON ct.contact_pk = hb.contact_pk
LEFT JOIN
  (SELECT
    contact_pk,
    contact_email
   FROM {{ ref('wh_contacts_dim') }},
   UNNEST( all_contact_emails ) as contact_email
    ) ce
ON ct.contact_pk = ce.contact_pk
WHERE
  ct.company_id = c.company_id )
SELECT
	contacts_dim.contact_email  AS email,
	contacts_dim.contact_name  AS name,
	contacts_web_interests_xa.last_page_title  AS last_page_title,
	CAST(contacts_web_interests_xa.last_visit_ts  AS DATE) AS last_visit_date,
	contacts_influencer_list_xa.influencer_status  AS influencer_status,
	COALESCE(SUM(contacts_web_interests_xa.attribution_interest ), 0) AS attribution_interest,
	COALESCE(SUM(contacts_web_interests_xa.casestudy_views ), 0) AS casestudy_views,
	COALESCE(SUM(contacts_web_interests_xa.customer_journey_interest ), 0) AS customer_journey_interest,
	COALESCE(SUM(contacts_web_interests_xa.data_centralisation_interest ), 0) AS data_centralisation_interest,
	COALESCE(SUM(contacts_web_interests_xa.data_teams_interest ), 0) AS data_teams_interest,
	COALESCE(SUM(contacts_web_interests_xa.dbt_interest ), 0) AS dbt_interest,
	COALESCE(SUM(contacts_web_interests_xa.looker_interest ), 0) AS looker_interest,
	COALESCE(SUM(contacts_web_interests_xa.personas_interest ), 0) AS personas_interest,
	COALESCE(SUM(contacts_web_interests_xa.pricing_views ), 0) AS pricing_views,
	COALESCE(SUM(contacts_web_interests_xa.ra_warehouse_interest ), 0) AS ra_warehouse_interest,
	COALESCE(SUM(contacts_web_interests_xa.segment_interest ), 0) AS segment_interest,
	COALESCE(SUM(contacts_web_interests_xa.visits_l90_days ), 0) AS visits_l90_days
FROM contacts_dim
LEFT JOIN {{ ref('wh_contacts_influencers_xa') }}
     AS contacts_influencer_list_xa ON contacts_dim.hubspot_contact_id = (concat('hubspot-',contacts_influencer_list_xa.hubspot_contact_id))
LEFT JOIN {{ ref('wh_contact_web_interests_xa') }}
     AS contacts_web_interests_xa ON contacts_dim.contact_pk = contacts_web_interests_xa.contact_pk

GROUP BY 1,2,3,4,5
  {% else %} {{config(enabled=false)}} {% endif %}
