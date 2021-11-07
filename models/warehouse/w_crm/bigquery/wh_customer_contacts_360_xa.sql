{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_company_sources") and var("projects_warehouse_delivery_sources") and var("crm_warehouse_contact_sources") and var("crm_warehouse_deal_sources") and var("crm_warehouse_conversations_sources") and var("marketing_warehouse_email_event_sources") and var("finance_warehouse_invoice_sources") and var("projects_warehouse_timesheet_sources")  %}
{{config(alias='customer_contacts_360_xa',
         materialized="view")}}

with contacts as (
  select contact_pk, contact_created_date, contact_name, job_title, ARRAY_CONCAT_AGG(contact_deals) as contact_deals from (
    SELECT cn.contact_pk, cn.contact_created_date, cn.contact_name, cn.job_title,  array_agg(struct(f.deal_pk, f.deal_name, 'Deal Created' as stage, f.deal_created_ts as deal_stage_ts, f.deal_amount as deal_amount, deal_type, deal_description, deal_source, deal_number_of_sprints, deal_components, deal_pricing_model, deal_partner_referral, deal_sprint_type, deal_products_in_solution, deal_pipeline_stage_id )) as contact_deals
    from {{ ref('wh_contacts_dim') }} cn
    JOIN {{ ref('wh_contact_deals_fact') }} d
    ON cn.contact_pk = d.contact_pk
    JOIN {{ ref('wh_deals_fact') }} f
    on d.deal_pk = f.deal_pk
    group by 1,2,3,4
    union all
    SELECT cn.contact_pk, cn.contact_created_date, cn.contact_name, cn.job_title,  array_agg(struct(f.deal_pk, f.deal_name, 'Deal Closed' as stage, f.deal_closed_ts as deal_stage_ts, f.deal_closed_amount_value as deal_amount, deal_type, deal_description, deal_source, deal_number_of_sprints, deal_components, deal_pricing_model, deal_partner_referral, deal_sprint_type, deal_products_in_solution, deal_pipeline_stage_id )) as contact_deals
    from {{ ref('wh_contacts_dim') }} cn
    JOIN {{ ref('wh_contact_deals_fact') }} d
    ON cn.contact_pk = d.contact_pk
    JOIN {{ ref('wh_deals_fact') }} f
    on d.deal_pk = f.deal_pk
    where f.deal_pipeline_stage_id like 'Closed Won%'
    group by 1,2,3,4
    union all
    SELECT cn.contact_pk, cn.contact_created_date, cn.contact_name, cn.job_title,  array_agg(struct(f.deal_pk, f.deal_name, 'Deal Lost' as stage, coalesce(f.deal_closed_ts, deal_last_modified_ts) as deal_stage_ts, f.deal_closed_amount_value as deal_amount, deal_type, deal_description, deal_source, deal_number_of_sprints, deal_components, deal_pricing_model, deal_partner_referral, deal_sprint_type, deal_products_in_solution, deal_pipeline_stage_id )) as contact_deals
    from {{ ref('wh_contacts_dim') }} cn
    JOIN {{ ref('wh_contact_deals_fact') }} d
    ON cn.contact_pk = d.contact_pk
    JOIN {{ ref('wh_deals_fact') }} f
    on d.deal_pk = f.deal_pk
    where f.deal_pipeline_stage_id = 'Closed Lost'
    group by 1,2,3,4)
group by 1,2,3,4
),
conversations as (
  select contact_pk, array_agg(struct(conversation_id,conversation_created_date, conversation_message_type, conversation_subject, conversation_body)) as contact_conversations
  from (
    SELECT cn.contact_pk, conversation_id, conversation_created_date,
    case when conversation_message_type = 'INCOMING_EMAIL' then 'Email Sent'
         when conversation_message_type = 'EMAIL' then 'Email Received'
         else conversation_message_type end as conversation_message_type , conversation_subject, conversation_body
    from {{ ref('wh_contacts_dim') }} cn
    JOIN {{ ref('wh_conversations_fact') }} v
    ON cn.contact_pk = v.contact_pk
    group by 1,2,3,4,5,6)
  group by 1
),
contact_conversations as (
  select cn.*,
         cv.contact_conversations
  from contacts cn
  left join conversations cv
  on cn.contact_pk = cv.contact_pk
  ),
email_events as (
  SELECT contact_pk, array_agg(struct(event_ts, c.ad_campaign_name, c.ad_network, concat('Marketing Email ',initcap(action)) as action, type, email_address, url)) email_event
  FROM {{ ref('wh_email_events_fact') }} e
  left outer join {{ ref('wh_ad_campaigns_dim') }} c
  on e.ad_campaign_pk = c.ad_campaign_pk
  where action not in ('deferred','delivered','processed','statuschange','stg_enrichment_clearbit_schema')
  group by 1
),
contact_conversations_email_events as (
  select cn.*,
         e.email_event
  from contact_conversations cn
  left join email_events e
  on cn.contact_pk = e.contact_pk
),
invoices as (
  select i.company_pk, array_agg(struct(invoice_pk, invoice_seq, i.timesheet_project_pk, invoice_event, invoice_event_ts, invoice_local_total_services_amount, p.project_name)) invoice
  from (
    SELECT company_pk, invoice_pk, invoice_seq, timesheet_project_pk, 'Invoice Created' as invoice_event, invoice_paid_at_ts as invoice_event_ts, invoice_local_total_services_amount, invoice_status, invoice_type
    FROM {{ ref('wh_invoices_fact') }}
    union all
    SELECT company_pk, invoice_pk, invoice_seq, timesheet_project_pk, 'Invoice Paid' as invoice_event, invoice_paid_at_ts as invoice_event_ts, invoice_local_total_services_amount, invoice_status, invoice_type
    FROM {{ ref('wh_invoices_fact') }}
    ) i
  join {{ ref('wh_timesheet_projects_dim') }} p
  on i.timesheet_project_pk = p.timesheet_project_pk
  where invoice_local_total_services_amount > 0
  and invoice_status != 'Voided'
  and invoice_type = 'Harvest - Client Billing'
  and invoice_event_ts is not null
  group by 1
),
delivery as (
  select company_pk, array_agg(struct(timesheet_billing_date,delivery_days,delivery_cost_amount)) as delivery
  from (
    SELECT
    	companies_dim.company_pk,
      project_timesheets.timesheet_billing_date,
      sum(project_timesheets.timesheet_hours_billed / 8) as delivery_days,
    	sum(project_timesheets.timesheet_hours_billed * coalesce(case when project_timesheets.timesheet_billable_hourly_cost_amount > 60 then 32 else project_timesheets.timesheet_billable_hourly_cost_amount end,25)) as delivery_cost_amount
    FROM {{ ref('wh_companies_dim') }} companies_dim
    JOIN {{ ref('wh_timesheet_projects_dim') }}
         AS projects_delivered ON companies_dim.company_pk = projects_delivered.company_pk
    JOIN {{ ref('wh_timesheets_fact') }}
         AS project_timesheets ON projects_delivered.timesheet_project_pk = project_timesheets.timesheet_project_pk
    GROUP BY 1,2)
  group by 1
)
SELECT c.company_pk, company_created_date, company_name, company_website, array_agg(struct(invoice)) invoice, array_agg(struct(delivery)) delivery, array_agg(struct(cn.contact_pk, contact_created_date, contact_name, job_title,contact_deals,contact_conversations, email_event)) contact
FROM {{ ref('wh_companies_dim') }} c
JOIN {{ ref('wh_contact_companies_fact') }} ccf
ON c.company_pk = ccf.company_pk
JOIN contact_conversations_email_events cn
ON ccf.contact_pk = cn.contact_pk
JOIN invoices i
on c.company_pk = i.company_pk
JOIN delivery d
on c.company_pk = d.company_pk
group by 1,2,3,4

{% else %} {{config(enabled=false)}} {% endif %}
