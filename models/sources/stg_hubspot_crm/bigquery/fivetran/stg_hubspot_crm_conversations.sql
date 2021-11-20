{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_conversations_sources") %}
{% if 'hubspot_crm' in var("crm_warehouse_conversations_sources") %}

with source AS (
  SELECT * from
  FROM {{ source('fivetran_hubspot_crm','engagements') }}

),
SELECT
  engagement_id                     AS message_id,
  companyids.value                  AS company_id,
  contactids.value                  AS contact_id,
  engagement.createdat              AS message_created_ts,
  engagement.type                   AS message_type_ts,
  engagement.ownerid                AS owner_id,
  dealids.value                     AS deal_id,
  engagement.timestamp              AS emessage_ts,
  metadata.status                   AS emessage_status,
  metadata.from.firstname           AS message_from_first_name,
  metadata.from.lastname            AS message_from_last_name,
  metadata.from.email               AS message_from_email,
  metadata.title                    AS message_title,
  metadata.subject                  AS message_subject,
  metadata_to.value.email           AS message_to_email,
  metadata.text                     AS message_text,
  engagement.lastupdated            AS message_lastupdated,
FROM
  source,
  unnest(associations.contactids) AS contactids,
  unnest(associations.companyids) AS companyids,
  unnest(associations.dealids) AS dealids,
  unnest(metadata.to) AS metadata_to
{{ dbt_utils.group_by(n=17) }}
)

)
SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
