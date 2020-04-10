with harvest_invoices as (
   select source,
          concat('harvest-',invoice_id) as invoice_id,
          company_id,
          invoice_number,
          invoice_project_id,
          invoice_company_id ,
          invoice_subject,
          invoice_issue_date,
          invoice_due_date,
          invoice_sent_at,
          invoice_created_at,
          invoice_creator_users_id,
          invoice_payment_term,
          invoice_period_start,
          invoice_period_end,
          invoice_paid_at,
          invoice_paid_date,
          invoice_local_total_amount,
          invoice_local_total_service_amount,
          invoice_local_total_licence_referral_fee_amount,
          invoice_local_total_expenses_amount,
          invoice_local_total_support_amount,
          from {{ ref('sde_harvest_projects_invoices')}}
),
    xero_invoices as (
      select source,
             concat('xero-',invoice_id) as invoice_id,
             invoice_number,
             invoice_reference,
             invoice_planned_payment_date,
             invoice_status,
             invoice_type,
             invoice_has_errors,
             invoice_due_date,
             invoice_url,
             invoice_sent_to_contact,
             invoice_is_discounted,
             invoice_date,
             invoice_fully_paid_on_date,
             invoice_line_amount_types,
             invoice_currency_code,
             invoice_currency_rate,
             invoice_contact_id,
             invoice_local_total_paid_amount,
             invoice_local_total_due_amount,
             invoice_local_total_credited_amount
             from {{ ref('sde_xero_accounting_invoices')}}
),
  joined_invoices as (
  select coalesce(cast(h.invoice_number as string),cast(x.invoice_number as string)) invoice_number,
         h.invoice_project_id as timesheet_project_id,
         h.invoice_id as harvest_invoice_id,
         x.invoice_id as xero_invoice_id,
         h.company_id,
         h.invoice_subject,
         h.invoice_issue_date,
         sum(h.invoice_local_total_amount) invoice_local_total_amount,
         sum(h.invoice_local_total_service_amount) invoice_local_total_service_amount,
         sum(h.invoice_local_total_licence_referral_fee_amount) invoice_local_total_licence_referral_fee_amount,
         sum(h.invoice_local_total_expenses_amount) invoice_local_total_expenses_amount,
         sum(h.invoice_local_total_support_amount) invoice_local_total_support_amount,
         h.invoice_due_date,
         h.invoice_sent_at,
         h.invoice_created_at,
         h.invoice_creator_users_id,
         x.invoice_contact_id,
         h.invoice_payment_term,
         h.invoice_period_start,
         h.invoice_period_end,
         x.invoice_reference,
         x.invoice_planned_payment_date,
         x.invoice_status,
         x.invoice_type,
         x.invoice_has_errors,
         x.invoice_currency_code,
         x.invoice_currency_rate,
         x.invoice_local_total_paid_amount,
         x.invoice_local_total_due_amount,
         x.invoice_local_total_credited_amount,
  from   harvest_invoices h
  join   xero_invoices x
  on     h.invoice_number = x.invoice_number
  group by 1,2,3,4,5,6,7,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30

),
 merged_invoice_ids as (
    select h.invoice_number, h.invoice_id
    from   harvest_invoices h
    union all
    select x.invoice_number, x.invoice_id
    from   xero_invoices x
 ),
  all_invoice_ids as (
    select invoice_number, array_agg(distinct invoice_id) as all_invoice_ids
    from   merged_invoice_ids
    group by 1
  )
  select j.invoice_number,
         a.all_invoice_ids,
         j.* except (invoice_number)
  from joined_invoices j
  join all_invoice_ids a
  on j.invoice_number = a.invoice_number
