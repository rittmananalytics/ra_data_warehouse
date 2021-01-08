{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_conversations_sources") %}
{% if 'intercom_messaging' in var("crm_warehouse_conversations_sources") %}

WITH source AS (
      {{ filter_stitch_table(var('stg_intercom_messaging_stitch_schema'),var('stg_intercom_messaging_stitch_conversations_table'),'id') }}
  ),
renamed as (
  SELECT
    concat('{{ var('stg_intercom_messaging_id-prefix') }}',id) as conversation_id,
    concat('{{ var('stg_intercom_messaging_id-prefix') }}',user.id) AS conversation_user_id,
    concat('{{ var('stg_intercom_messaging_id-prefix') }}',conversation_message.author.id) AS conversation_author_id,
    cast (null as string) as company_id,
    conversation_message.author.type AS conversation_author_type,
    user.type AS  conversation_user_type,
    concat('{{ var('stg_intercom_messaging_id-prefix') }}',assignee.id) AS conversation_assignee_id,
    assignee.type  AS conversation_assignee_state,
    concat('{{ var('stg_intercom_messaging_id-prefix') }}',conversation_message.id) AS conversation_message_id,
    conversation_message.type AS  conversation_message_type,
    conversation_message.body AS conversation_body,
    conversation_message.subject as  conversation_subject,
    created_at as contact_created_date,
    updated_at as contact_last_modified_date,
    read AS is_conversation_read,
    open AS is_conversation_open,
    null as deal_id
  FROM
    source)
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
