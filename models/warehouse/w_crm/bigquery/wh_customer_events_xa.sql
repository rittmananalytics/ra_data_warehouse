{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_company_sources") %}
{{config(alias='customer_events_xa',
         materialized="view")}}
with events AS (SELECT company_pk,
       company_name,
       company_created_date AS event_ts,
       'Client Created' AS event_type,
       company_name AS event_details,
       0 AS event_value,
       null AS event_contact_pk,
       null AS event_contact_name
FROM {{ ref('wh_customer_contacts_360_xa') }}
{% if var("finance_warehouse_invoice_sources") %}
union all
SELECT company_pk,
       company_name,
       invoice.invoice_event_ts AS event_ts,
       invoice.invoice_event AS event_type,
       invoice.project_name AS event_details,
       invoice.invoice_local_total_services_amount AS event_value,
       CAST(null AS {{ dbt_utils.type_string() }}) AS event_contact_pk,
       CAST(null AS {{ dbt_utils.type_string() }}) AS event_contact_name
FROM {{ ref('wh_customer_contacts_360_xa') }} ,
unnest(invoice) AS invoice1,
unnest(invoice1.invoice) AS invoice
group by 1,2,3,4,5,6,7,8
{% endif %}
{% if var("crm_warehouse_contact_sources") %}
union all
SELECT company_pk,
       company_name,
       contact.contact_created_date AS event_ts,
       'Contact Created' AS event_type,
       contact.contact_name AS event_details,
       0 AS event_value,
       contact.contact_pk AS event_contact_pk,
       contact.contact_name AS event_contact_name
FROM {{ ref('wh_customer_contacts_360_xa') }} ,
unnest(contact) AS contact
{% endif %}
{% if var("crm_warehouse_deal_sources") and var("crm_warehouse_contact_sources") %}
union all
SELECT company_pk,
       company_name,
       deals.deal_stage_ts AS event_ts,
       deals.stage AS event_type,
       deals.deal_name AS event_details,
       deals.deal_amount AS event_value,
       contact.contact_pk AS event_contact_pk,
       contact.contact_name AS event_contact_name
FROM {{ ref('wh_customer_contacts_360_xa') }} ,
unnest(contact) AS contact,
unnest(contact.contact_deals) AS deals
{% endif %}
{% if var("crm_warehouse_conversations_sources") and var("crm_warehouse_companies_sources")  %}
union all
SELECT company_pk,
       company_name,
       conversations.conversation_created_date AS event_ts,
       conversations.conversation_message_type AS event_type,
       left(replace(regexp_replace(conversations.conversation_body, '[^a-zA-Z0-9]', ' '),'  ',' '),200) AS event_details,
       0 AS event_value,
       contact.contact_pk AS event_contact_pk,
       contact.contact_name AS event_contact_name
FROM {{ ref('wh_customer_contacts_360_xa') }} ,
unnest(contact) AS contact,
unnest(contact.contact_conversations) AS conversations
{% endif %}
{% if var("marketing_warehouse_email_event_sources") %}
union all
SELECT company_pk,
       company_name,
       email_event.event_ts AS event_ts,
       email_event.action AS event_type,
       coalesce(url,type,ad_campaign_name) AS event_details,
       0 AS event_value,
       contact.contact_pk AS event_contact_pk,
       contact.contact_name AS event_contact_name
FROM {{ ref('wh_customer_contacts_360_xa') }} ,
unnest(contact) AS contact,
unnest(contact.email_event) AS email_event
{% endif %}
{% if var("projects_warehouse_delivery_sources") %}
union all
SELECT * FROM (SELECT company_pk,
       company_name,
       delivery.timesheet_billing_date AS event_ts,
       'Delivery Cost' AS event_type,
       CAST(delivery.delivery_days AS string) AS event_details,
       delivery.delivery_cost_amount AS event_value,
       CAST(null AS {{ dbt_utils.type_string() }}) AS event_contact_pk,
       CAST(null AS {{ dbt_utils.type_string() }}) AS event_contact_name
FROM {{ ref('wh_customer_contacts_360_xa') }},
unnest(delivery) AS delivery1,
unnest(delivery1.delivery) AS delivery)
group by 1,2,3,4,5,6,7,8
{% endif %}
)
SELECT *,
       row_number() over (PARTITION BYcompany_pk order by event_ts) AS client_event_seq,
       date_diff(date(event_ts), min(date(event_ts)) over (PARTITION BYcompany_pk),MONTH) AS months_since_client_created,
       min(date(event_ts)) over (PARTITION BYcompany_pk) AS client_cohort_month,
       case when event_type = 'Invoice Paid' then event_value
            when event_type = 'Delivery Cost' then -1*event_value
            else 0 end AS total_ltv
FROM events
{% else %} {{config(enabled=false)}} {% endif %}
