{% if not var("enable_crm_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}


with conversations_merge_list as
  (
    {% if var("enable_hubspot_crm_source") %}

    SELECT *
    FROM   {{ ref('stg_hubspot_crm_conversations') }}

    {% endif %}
    {% if var("enable_intercom_messaging_source") and var("enable_hubspot_crm_source")  %}
    UNION ALL
    {% endif %}
    {% if var("enable_intercom_messaging_source")  %}

    SELECT *
    FROM   {{ ref('stg_intercom_messaging_conversations') }}

    {% endif %}
  )
select * from conversations_merge_list
