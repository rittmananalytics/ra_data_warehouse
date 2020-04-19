{% if not var("enable_hubspot_crm_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH
  deduped_communications AS (
  SELECT
    *
  FROM (
    SELECT
      *,
      MAX(_sdc_batched_at) OVER (PARTITION BY engagement_id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
    FROM
      {{ source('hubspot_crm', 'engagements') }} )
  WHERE
    _sdc_batched_at = max_sdc_batched_at),
  owners AS (
  SELECT
    *
  FROM (
    SELECT
      *,
      MAX(_sdc_batched_at) OVER (PARTITION BY ownerid ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
    FROM
      {{ source('hubspot_crm', 'owners') }} )
  WHERE
    _sdc_batched_at = max_sdc_batched_at)
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
