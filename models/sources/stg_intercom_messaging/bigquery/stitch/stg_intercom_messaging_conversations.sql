{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_conversations_sources") %}
{% if 'intercom_messaging' in var("crm_warehouse_conversations_sources") %}

WITH source AS (
      {{ filter_stitch_relation(relation=source('stitch_intercom_messaging', 'conversations'),unique_column='id') }}
  ),
renamed AS (
  SELECT
    CONCAT('{{ var('stg_intercom_messaging_id-prefix') }}',id) AS conversation_id,
    CONCAT('{{ var('stg_intercom_messaging_id-prefix') }}',user.id) AS conversation_user_id,
    CONCAT('{{ var('stg_intercom_messaging_id-prefix') }}',conversation_message.author.id) AS conversation_author_id,
    CAST(null AS {{ dbt_utils.type_string() }}) AS company_id,
    conversation_message.author.type AS conversation_author_type,
    user.type AS  conversation_user_type,
    CONCAT('{{ var('stg_intercom_messaging_id-prefix') }}',assignee.id) AS conversation_assignee_id,
    assignee.type  AS conversation_assignee_state,
    CONCAT('{{ var('stg_intercom_messaging_id-prefix') }}',conversation_message.id) AS conversation_message_id,
    conversation_message.type AS  conversation_message_type,
    conversation_message.body AS conversation_body,
    conversation_message.subject AS  conversation_subject,
    created_at AS contact_created_date,
    updated_at AS contact_last_modified_date,
    read AS is_conversation_read,
    open AS is_conversation_open,
    null AS deal_id
  FROM
    source)
SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
