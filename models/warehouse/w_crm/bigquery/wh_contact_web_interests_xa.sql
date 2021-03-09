{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contact_sources") and var("product_warehouse_event_sources") %}
{{config(alias='contacts_web_interests_xa',
         materialized="view")}}
WITH
  base_scores AS (
  SELECT
    *,
    CASE
      WHEN event_details LIKE '%Attribution%' OR page_url LIKE '%attribution%' THEN 1
  END
    AS count_attribution,
    CASE
      WHEN event_details LIKE '%RA Warehouse%' THEN 1
  END
    AS count_ra_warehouse,
    CASE
      WHEN event_details LIKE '%Pricing%' OR page_url LIKE '%pricing%' THEN 1
  END
    AS count_pricing,
    CASE
      WHEN event_details LIKE '%Looker%' OR page_url LIKE '%looker%'THEN 1
  END
    AS count_looker,
    CASE
      WHEN event_details LIKE '%Oracle Autonomous Data Warehouse Cloud%' THEN 1
  END
    AS count_oawc,
    CASE
      WHEN event_details LIKE '%dbt%' OR page_url LIKE '%dbt%' THEN 1
  END
    AS count_dbt,
    CASE
      WHEN event_details LIKE '%Customer Journey%' THEN 1
  END
    AS count_customer_journey,
    CASE
      WHEN event_details LIKE '%Data Centralization%' OR event_details LIKE '%Data Centralisation%' OR page_url LIKE '%central%' THEN 1
  END
    AS count_data_centralisation,
    CASE
      WHEN event_details LIKE '%Ad Spend%' THEN 1
  END
    AS count_marketing_analytics,
    CASE
      WHEN event_details LIKE '%Segment%' OR page_url LIKE '%segment%' THEN 1
  END
    AS count_segment,
    CASE
      WHEN event_details LIKE '%Modern BI Stack%' OR event_details LIKE '%Modern Data Stack%' THEN 1
  END
    AS count_modern_data_stack,
    CASE
      WHEN page_url LIKE '%/blog%' THEN 1
  END
    AS count_blog,
    CASE
      WHEN page_url LIKE '%/podcast%' THEN 1
  END
    AS count_podcast,
    CASE
      WHEN page_url LIKE '%/highgrowth%' THEN 1
  END
    AS count_startup,
    CASE
      WHEN event_details LIKE '%Marketing Automation%' THEN 1
  END
    AS count_martech,
    CASE
      WHEN event_details LIKE '%Personas%' OR page_url LIKE '%personas%' THEN 1
  END
    AS count_personas,
    CASE
      WHEN event_details LIKE '%Google BigQuery%' OR page_url LIKE '%bigquery%' THEN 1
  END
    AS count_bigquery,
    CASE
      WHEN event_details LIKE '%Data Teams%' OR event_details LIKE '%Data Strategy%' THEN 1
  END
    AS count_data_teams,
    CASE
      WHEN page_url LIKE '%/customer%' THEN 1
  END
    AS count_casestudy,
    CASE
      WHEN event_type LIKE '%Button%' THEN 1
  END
    AS count_button_pressed,
    CASE
      WHEN event_type = 'Page View' THEN 1
      WHEN event_type IN ('Clicked Link',
      'Pricing View') THEN 2
      WHEN event_type = 'Email Link Clicked' THEN 3
      WHEN event_type LIKE '%Button%' THEN 4
    ELSE
    0
  END
    AS multiplier
  FROM
    {{ ref('wh_contact_web_event_history')}}),
  weighted_scores AS (
  SELECT
    contact_pk,
    count_blog,
    count_podcast,
    count_pricing,
    count_casestudy,
    count_attribution * multiplier AS weighted_count_attribution,
    count_ra_warehouse * multiplier AS weighted_count_ra_warehouse,
    count_looker * multiplier AS weighted_count_looker,
    count_oawc * multiplier AS weighted_count_oawc,
    count_dbt * multiplier AS weighted_count_dbt,
    count_customer_journey * multiplier AS weighted_count_customer_journey,
    count_data_centralisation * multiplier AS weighted_count_data_centralisation,
    count_marketing_analytics * multiplier AS weighted_count_marketing_analytics,
    count_segment * multiplier AS weighted_count_segment,
    count_modern_data_stack * multiplier AS weighted_count_modern_data_stack,
    count_startup * multiplier AS weighted_count_startup,
    count_martech * multiplier AS weighted_count_martech,
    count_personas * multiplier AS weighted_count_personas,
    count_bigquery * multiplier AS weighted_count_bigquery,
    count_data_teams * multiplier AS weighted_count_data_teams
  FROM
    base_scores),
  total_scores AS (
  SELECT
    contact_pk,
    coalesce(SUM(count_pricing),
      0) AS pricing_views,
    coalesce(SUM(count_blog),
      0) AS blog_views,
    coalesce(SUM(count_podcast),
      0) AS podcast_views,
    coalesce(SUM(weighted_count_attribution),
      0) AS attribution_interest,
    coalesce(SUM(weighted_count_ra_warehouse),
      0) AS ra_warehouse_interest,
    coalesce(SUM(weighted_count_looker),
      0) AS looker_interest,
    coalesce(SUM(weighted_count_oawc),
      0) AS oawc_interest,
    coalesce(SUM(weighted_count_dbt),
      0) AS dbt_interest,
    coalesce(SUM(weighted_count_customer_journey),
      0) AS customer_journey_interest,
    coalesce(SUM(weighted_count_data_centralisation),
      0) AS data_centralisation_interest,
    coalesce(SUM(weighted_count_marketing_analytics),
      0) AS marketing_analytics_interest,
    coalesce(SUM(weighted_count_segment),
      0) AS segment_interest,
    coalesce(SUM(weighted_count_modern_data_stack),
      0) AS modern_data_stack_interest,
    coalesce(SUM(weighted_count_startup),
      0) AS startup_interest,
    coalesce(SUM(weighted_count_martech),
      0) AS martech_interest,
    coalesce(SUM(weighted_count_personas),
      0) AS personas_interest,
    coalesce(SUM(weighted_count_bigquery),
      0) AS bigquery_interest,
    coalesce(SUM(weighted_count_data_teams),
      0) AS data_teams_interest,
    coalesce(SUM(count_casestudy),
      0) AS casestudy_views
  FROM
    weighted_scores
  GROUP BY
    1 ),
  last_visit AS (
  SELECT
    contact_pk,
    event_ts,
    page_title
  FROM (
    SELECT
      contact_pk,
      event_ts,
      page_title,
      ROW_NUMBER() OVER (PARTITION BY contact_pk ORDER BY event_ts DESC) AS visit_seq_desc
    FROM
      {{ ref('wh_contact_web_event_history')}}
    WHERE
      event_type = 'Page View')
  WHERE
    visit_seq_desc = 1),
  visits_last_90_days AS (
  SELECT
    contact_pk,
    COUNT(*) AS visits_l90_days
  FROM
    {{ ref('wh_contact_web_event_history')}}
  WHERE
    event_ts >= TIMESTAMP_SUB(current_timestamp,INTERVAL 90 day)
    AND event_type = 'Page View'
  GROUP BY
    1 )
SELECT
  t.*,
  l.event_ts AS last_visit_ts,
  l.page_title AS last_page_title,
  coalesce(v.visits_l90_days) AS visits_l90_days
FROM
  total_scores t
LEFT JOIN
  last_visit l
ON
  t.contact_pk = l.contact_pk
LEFT JOIN
  visits_last_90_days v
ON
  t.contact_pk = v.contact_pk
  {% else %} {{config(enabled=false)}} {% endif %}
