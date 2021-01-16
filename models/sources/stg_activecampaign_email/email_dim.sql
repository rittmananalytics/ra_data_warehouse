{{
    config(
        materialized='table'
    )
}}

WITH source AS
  (SELECT campaign_name,
          campaign_created_date,
          campaign_send_date,
          campaign_id,
          campaign_author_id
   FROM {{ ref('base_activecampaign_campaigns') }}), 

messages AS
  (SELECT *
   EXCEPT (highest_percentage,
           percentage)
   FROM
     (SELECT campaign_id,
             message_id,
             message_subject,
             max(percentage) OVER (PARTITION BY campaign_id) AS highest_percentage,
                                  percentage
      FROM {{ ref('base_activecampaign_messages') }})
   WHERE percentage = highest_percentage ),

renamed AS
  (SELECT MAX(campaign_name) AS content_name,
          concat("campaign-", s.campaign_id) AS content_id,
          MIN("email campaign") AS content_type,
          MIN(campaign_created_date) AS date_published,
          MAX(m.message_subject) AS subject,
   FROM SOURCE s
   LEFT JOIN messages m ON s.campaign_id = m.campaign_id
   GROUP BY s.campaign_id)

SELECT *
FROM renamed