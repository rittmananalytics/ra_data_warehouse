{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_conversations_sources") %}
{% if 'hubspot_crm' in var("crm_warehouse_conversations_sources") %}

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

)
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
