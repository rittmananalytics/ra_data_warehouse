{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_company_sources") %}
{{config(alias='customer_events_xa',
         materialized="view")}}
with events as (SELECT company_pk,
       company_name,
       company_created_date as event_ts,
       'Client Created' as event_type,
       company_name as event_details,
       0 as event_value,
       null as event_contact_pk,
       null as event_contact_name
FROM {{ ref('wh_customer_contacts_360_xa') }}
{% if var("finance_warehouse_invoice_sources") %}
union all
SELECT company_pk,
       company_name,
       invoice.invoice_event_ts as event_ts,
       invoice.invoice_event as event_type,
       invoice.project_name as event_details,
       invoice.invoice_local_total_services_amount as event_value,
       cast (null as string) as event_contact_pk,
       cast (null as string) as event_contact_name
FROM {{ ref('wh_customer_contacts_360_xa') }} ,
unnest(invoice) as invoice1,
unnest(invoice1.invoice) as invoice
group by 1,2,3,4,5,6,7,8
{% endif %}
{% if var("crm_warehouse_contact_sources") %}
union all
SELECT company_pk,
       company_name,
       contact.contact_created_date as event_ts,
       'Contact Created' as event_type,
       contact.contact_name as event_details,
       0 as event_value,
       contact.contact_pk as event_contact_pk,
       contact.contact_name as event_contact_name
FROM {{ ref('wh_customer_contacts_360_xa') }} ,
unnest(contact) as contact
{% endif %}
{% if var("crm_warehouse_deal_sources") and var("crm_warehouse_contact_sources") %}
union all
SELECT company_pk,
       company_name,
       deals.deal_stage_ts as event_ts,
       deals.stage as event_type,
       deals.deal_name as event_details,
       deals.deal_amount as event_value,
       contact.contact_pk as event_contact_pk,
       contact.contact_name as event_contact_name
FROM {{ ref('wh_customer_contacts_360_xa') }} ,
unnest(contact) as contact,
unnest(contact.contact_deals) as deals
{% endif %}
{% if var("crm_warehouse_conversations_sources") and var("crm_warehouse_companies_sources")  %}
union all
SELECT company_pk,
       company_name,
       conversations.conversation_created_date as event_ts,
       conversations.conversation_message_type as event_type,
       left(replace(regexp_replace(conversations.conversation_body, '[^a-zA-Z0-9]', ' '),'  ',' '),200) as event_details,
       0 as event_value,
       contact.contact_pk as event_contact_pk,
       contact.contact_name as event_contact_name
FROM {{ ref('wh_customer_contacts_360_xa') }} ,
unnest(contact) as contact,
unnest(contact.contact_conversations) as conversations
{% endif %}
{% if var("marketing_warehouse_email_event_sources") %}
union all
SELECT company_pk,
       company_name,
       email_event.event_ts as event_ts,
       email_event.action as event_type,
       coalesce(url,type,ad_campaign_name) as event_details,
       0 as event_value,
       contact.contact_pk as event_contact_pk,
       contact.contact_name as event_contact_name
FROM {{ ref('wh_customer_contacts_360_xa') }} ,
unnest(contact) as contact,
unnest(contact.email_event) as email_event
{% endif %}
{% if var("projects_warehouse_delivery_sources") %}
union all
select * from (SELECT company_pk,
       company_name,
       delivery.timesheet_billing_date as event_ts,
       'Delivery Cost' as event_type,
       cast(delivery.delivery_days as string) as event_details,
       delivery.delivery_cost_amount as event_value,
       cast(null as string) as event_contact_pk,
       cast(null as string) as event_contact_name
FROM {{ ref('wh_customer_contacts_360_xa') }},
unnest(delivery) as delivery1,
unnest(delivery1.delivery) as delivery)
group by 1,2,3,4,5,6,7,8
{% endif %}
)
select *,
       row_number() over (partition by company_pk order by event_ts) as client_event_seq,
       date_diff(date(event_ts), min(date(event_ts)) over (partition by company_pk),MONTH) as months_since_client_created,
       min(date(event_ts)) over (partition by company_pk) as client_cohort_month,
       case when event_type = 'Invoice Paid' then event_value
            when event_type = 'Delivery Cost' then -1*event_value
            else 0 end as total_ltv
from events
{% else %} {{config(enabled=false)}} {% endif %}
