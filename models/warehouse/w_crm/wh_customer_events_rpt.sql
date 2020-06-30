{% if not var("enable_crm_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        alias='customer_events_rpt'
    )
}}
{% endif %}
with events as (
SELECT
  c.company_pk,
  c.company_name,
  'New Deal Created' as event_type,
  d.deal_created_date as event_ts,
  d.deal_name as event_details,
  d.deal_amount as event_value
FROM
  {{ ref('wh_companies_dim')}} c
JOIN
  {{ ref('wh_deals_fact')}} d
ON
  c.company_pk = d.company_pk
UNION ALL
SELECT
  c.company_pk,
  c.company_name,
  'Deal Closed Won' as event_type,
  d.deal_closed_date as event_ts,
  d.deal_name as event_details,
  d.deal_amount as event_value
FROM
  {{ ref('wh_companies_dim')}} c
JOIN
  {{ ref('wh_deals_fact')}} d
ON
  c.company_pk = d.company_pk
WHERE
  d.pipeline_stage_label = 'Closed Won and Delivered'
UNION ALL
SELECT
  c.company_pk,
  c.company_name,
  'Project Hours Logged' as event_type,
  t.timesheet_billing_date as event_ts,
  p.project_name as event_details,
  t.timesheet_hours_billed as event_value
FROM
  {{ ref('wh_companies_dim')}} c
JOIN
  {{ ref('wh_timesheets_fact')}} t
ON
  c.company_pk = t.company_pk
JOIN
{{ ref('wh_timesheet_projects_dim')}} p
ON
 t.timesheet_project_pk = p.timesheet_project_pk
WHERE
  t.timesheet_is_billable
UNION ALL
SELECT
  c.company_pk,
  c.company_name,
  'Client Invoiced' as event_type,
  i. invoice_issue_at_ts as event_ts,
  i.invoice_subject as event_details,
  case when i.invoice_currency = 'USD' then i.invoice_local_total_revenue_amount *.8
       when i.invoice_currency = 'EUR' then i.invoice_local_total_revenue_amount *.9
       else i.invoice_local_total_revenue_amount end as event_value
FROM
  {{ ref('wh_companies_dim') }} c
JOIN
{{ ref('wh_invoices_fact') }} i
ON
  c.company_pk = i.company_pk
WHERE
  i.invoice_status != 'Void')
select *,
       row_number() over (order by event_ts) as event_pk
       from events
