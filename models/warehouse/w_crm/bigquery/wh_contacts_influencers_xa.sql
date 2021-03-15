{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_company_sources") and var("crm_warehouse_deal_sources") and var("crm_warehouse_contact_sources") and var("enable_looker_usage_source") %}
{{config(alias='contacts_influencer_list_xa',
         materialized="view")}}
WITH
  contacts_dim AS (
  SELECT
    ct.*,
    c.company_pk
  FROM (
    SELECT
      *
    FROM
      {{ ref('wh_contacts_dim') }},
      UNNEST( all_contact_company_ids) AS company_id,
      UNNEST( all_contact_ids) as contact_id) ct
  JOIN (
    SELECT
      *
    FROM
      {{ ref('wh_companies_dim') }} c,
      UNNEST (all_company_ids) AS company_id ) c
  ON
    ct.company_id = c.company_id
  WHERE
    ct.company_id = c.company_id )
    ,
  looker_usage AS (
  SELECT
    looker_users_dim.contact_name AS contact_name,
    looker_users_dim.contact_id,
    COALESCE(SUM(looker_usage_fact.approximate_web_usage_in_minutes ),
      0) AS historic_looker_usage_in_minutes
  FROM
    {{ ref('wh_companies_dim') }} companies_dim
  INNER JOIN
    {{ ref('wh_looker_usage_fact') }} AS looker_usage_fact
  ON
    companies_dim.company_pk = looker_usage_fact.company_pk
  LEFT JOIN
    contacts_dim AS looker_users_dim
  ON
    looker_usage_fact.contact_pk = looker_users_dim.contact_pk
  GROUP BY
    1,2)
    ,
  associated_deals AS (
  SELECT
    customer_events_xa.event_contact_name AS contact_name,
    COALESCE(SUM(CAST(customer_events_xa.event_value AS FLOAT64)),
      0) AS historic_opportunity_value
  FROM
    {{ ref('wh_companies_dim') }} companies_dim
  INNER JOIN
    {{ ref('wh_customer_events_xa') }} AS customer_events_xa
  ON
    companies_dim.company_pk = customer_events_xa.company_pk
  WHERE
    (customer_events_xa.event_type = 'Deal Created')
  GROUP BY
    1),
  contact_scores AS (
  SELECT
    coalesce(l.contact_name,

      d.contact_name) AS contact_name,
      l.contact_id,
    coalesce(l.historic_looker_usage_in_minutes,
      0) AS historic_looker_usage_in_minutes,
    coalesce(d.historic_opportunity_value,
      0) AS historic_opportunity_value,
    NTILE(4) OVER (ORDER BY coalesce(l.historic_looker_usage_in_minutes, 0) ) AS historic_looker_usage_ntile,
    NTILE(4) OVER (ORDER BY coalesce(d.historic_opportunity_value, 0) ) AS historic_opportunity_value_ntile,
    NTILE(4) OVER (ORDER BY coalesce(l.historic_looker_usage_in_minutes, 0) ) + NTILE(4) OVER (ORDER BY coalesce(d.historic_opportunity_value, 0) ) AS contact_influencer_score
  FROM
    looker_usage l
  LEFT JOIN
    associated_deals d
  ON
    l.contact_name = d.contact_name
  WHERE
    coalesce(l.contact_name,
      d.contact_name) IS NOT NULL
  and contact_id like '%hubspot%')
SELECT
  contact_name,
  replace(contact_id,'hubspot-','') as hubspot_contact_id,
  contact_influencer_score,
  CASE
    WHEN contact_influencer_score <= 4 THEN 'None'
    WHEN contact_influencer_score BETWEEN 4
  AND 6 THEN 'Influencer'
    WHEN contact_influencer_score >= 7 THEN 'Champion'
END
  AS influencer_status
FROM
  contact_scores
  {% else %} {{config(enabled=false)}} {% endif %}
