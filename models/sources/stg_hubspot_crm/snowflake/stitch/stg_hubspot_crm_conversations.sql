{{config(enabled = target.type == 'snowflake')}}
{% if var("crm_warehouse_conversations_sources") %}
{% if 'hubspot_crm' in var("crm_warehouse_conversations_sources") %}

{% if var("stg_hubspot_crm_etl") == 'fivetran' %}
with source AS (
  SELECT * from
  FROM {{ var('stg_hubspot_crm_fivetran_engagements_table') }}

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
{% elif var("stg_hubspot_crm_etl") == 'stitch' %}
with source AS (
  {{ filter_stitch_relation(relation=var('stg_hubspot_crm_stitch_engagements_table'),unique_column='engagement_id') }}
),
renamed AS (
SELECT
  CONCAT('{{ var('stg_hubspot_crm_id-prefix') }}',engagement_id)                    AS conversation_id,
  CONCAT('{{ var('stg_hubspot_crm_id-prefix') }}',contactids.value)                  AS conversation_user_id,
  CONCAT('{{ var('stg_hubspot_crm_id-prefix') }}',contactids.value)                  AS conversation_author_id,
  CONCAT('{{ var('stg_hubspot_crm_id-prefix') }}',companyids.value)                  AS company_id,
  CAST(null AS {{ dbt_utils.type_string() }}) 		AS conversation_author_type,
  CAST(null AS {{ dbt_utils.type_string() }})  AS  conversation_user_type,
  CONCAT('{{ var('stg_hubspot_crm_id-prefix') }}',contactids.value)  AS conversation_assignee_id,
  CAST(null AS {{ dbt_utils.type_string() }})    AS conversation_assignee_state,
  CONCAT('{{ var('stg_hubspot_crm_id-prefix') }}',engagement_id )  AS conversation_message_id,
  coalesce(engagement.type,CAST(null AS {{ dbt_utils.type_string() }}))   AS  conversation_message_type,
  coalesce(metadata.text,CAST(null AS {{ dbt_utils.type_string() }}))    AS conversation_body,
  coalesce(metadata.subject,CAST(null AS {{ dbt_utils.type_string() }}))    AS  conversation_subject,
  engagement.createdat AS conversation_created_date,
  engagement.lastupdated AS contact_last_modified_date,
  CAST(null AS {{ dbt_utils.type_boolean() }}) AS is_conversation_read,
  CAST(null AS {{ dbt_utils.type_boolean() }}) AS is_conversation_open,
  dealids.value                    AS deal_id
FROM
  source,
  unnest(associations.contactids) AS contactids,
  unnest(associations.companyids) AS companyids,
  unnest(associations.dealids) AS dealids,
  unnest(metadata.to) AS metadata_to
{{ dbt_utils.group_by(n=17) }}
{% endif %}
)
SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
