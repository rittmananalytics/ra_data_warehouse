{{config
  (enabled =
      (target.type == 'bigquery' and var("stg_hubspot_crm_etl") == 'stitch')
   )
}}
{% if var("crm_warehouse_conversations_sources") %}
{% if 'hubspot_crm' in var("crm_warehouse_conversations_sources") %}


with source AS (
  {{ filter_stitch_relation(relation=source('stitch_hubspot_crm','engagements'),unique_column='engagement_id') }}
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
)
SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
