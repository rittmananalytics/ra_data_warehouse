{% if not var("enable_hubspot_crm_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH
  deduped_communications AS (
  {{ filter_source('hubspot_crm','s_engagements','engagement_id') }}
  ),
  owners AS (
  {{ filter_source('hubspot_crm','s_owners','ownerid') }}
  )
SELECT
  engagement_id AS communication_id,
  concat(cast(engagement_id as string),coalesce(cast(associations.companyids[OFFSET(off)] as string),'')) as communication_uid,
  associations.companyids[OFFSET(off)] AS company_id,
  associations.dealids AS deal_id,
  engagement.timestamp AS communication_ts,
  engagement.ownerid AS communication_owner_id,
  engagement.type AS communication_type,
  metadata.text AS communication_text,
  metadata.subject AS communication_subject,
  CONCAT(CONCAT(metadata.FROM.firstname,' '), metadata.FROM.lastname) AS communication_from_firstname_lastname,
  metadata.status AS communication_status,
  metadatato.email AS communication_to_email,
FROM (
  SELECT
    deduped_communications.*
  FROM
    deduped_communications
  LEFT OUTER JOIN
    owners
  ON
    deduped_communications.engagement.ownerid = owners.ownerid) communications,
  UNNEST(communications.associations.companyids) WITH OFFSET off
LEFT JOIN
  communications.associations.dealids
LEFT JOIN
  communications.metadata.TO metadatato
