{{config(enabled = target.type == 'snowflake')}}
{% if var("crm_warehouse_conversations_sources") %}
{% if 'hubspot_crm' in var("crm_warehouse_conversations_sources") %}

{% if var("stg_hubspot_crm_etl") == 'fivetran' %}
with source as (
  select * from
  from {{ var('stg_hubspot_crm_fivetran_engagements_table') }}

),
SELECT
  engagement_id                     as message_id,
  companyids.value                  as company_id,
  contactids.value                  as contact_id,
  engagement.createdat              as message_created_ts,
  engagement.type                   as message_type_ts,
  engagement.ownerid                as owner_id,
  dealids.value                     as deal_id,
  engagement.timestamp              as emessage_ts,
  metadata.status                   as emessage_status,
  metadata.from.firstname           as message_from_first_name,
  metadata.from.lastname            as message_from_last_name,
  metadata.from.email               as message_from_email,
  metadata.title                    as message_title,
  metadata.subject                  as message_subject,
  metadata_to.value.email           as message_to_email,
  metadata.text                     as message_text,
  engagement.lastupdated            as message_lastupdated,
FROM
  source,
  unnest(associations.contactids) as contactids,
  unnest(associations.companyids) as companyids,
  unnest(associations.dealids) as dealids,
  unnest(metadata.to) as metadata_to
{{ dbt_utils.group_by(n=17) }}
)
{% elif var("stg_hubspot_crm_etl") == 'stitch' %}
with source as (
  {{ filter_stitch_relation(relation=var('stg_hubspot_crm_stitch_engagements_table'),unique_column='engagement_id') }}
),
renamed as (
SELECT
  concat('{{ var('stg_hubspot_crm_id-prefix') }}',engagement_id)                    as conversation_id,
  concat('{{ var('stg_hubspot_crm_id-prefix') }}',contactids.value)                  as conversation_user_id,
  concat('{{ var('stg_hubspot_crm_id-prefix') }}',contactids.value)                  as conversation_author_id,
  concat('{{ var('stg_hubspot_crm_id-prefix') }}',companyids.value)                  as company_id,
  cast (null as string) 		AS conversation_author_type,
  cast (null as string)  AS  conversation_user_type,
  concat('{{ var('stg_hubspot_crm_id-prefix') }}',contactids.value)  AS conversation_assignee_id,
  cast (null as string)    AS conversation_assignee_state,
  concat('{{ var('stg_hubspot_crm_id-prefix') }}',engagement_id )  AS conversation_message_id,
  coalesce(engagement.type,cast(null as string))   AS  conversation_message_type,
  coalesce(metadata.text,cast(null as string))    AS conversation_body,
  coalesce(metadata.subject,cast(null as string))    as  conversation_subject,
  engagement.createdat as conversation_created_date,
  engagement.lastupdated as contact_last_modified_date,
  cast(null as boolean) AS is_conversation_read,
  cast(null as boolean) AS is_conversation_open,
  dealids.value                    as deal_id
FROM
  source,
  unnest(associations.contactids) as contactids,
  unnest(associations.companyids) as companyids,
  unnest(associations.dealids) as dealids,
  unnest(metadata.to) as metadata_to
{{ dbt_utils.group_by(n=17) }}
{% endif %}
)
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
