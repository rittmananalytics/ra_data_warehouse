with sde_harvest_projects_invoices_ds as (

   select source,
          invoice_id as harvest_invoice_id,
          company_id,
          invoice_number,
          harvest_project_id,
          harvest_company_id ,
          invoice_subject,
          invoice_local_revenue_amount_billed,
          invoice_local_amount,
          invoice_currency,
          invoice_total_local_amount_billed,
          invoice_local_services_amount_billed,
          invoice_local_license_referral_fee_amount_billed,
          invoice_expenses_amount_billed,
          invoice_support_amount_billed,
          invoice_tax_billed,
          invoice_issue_date,
          invoice_due_date,
          invoice_sent_at,
          invoice_created_at,
          invoice_creator_id,
          invoice_tax_amount,
          invoice_due_amount,
          invoice_tax,
          invoice_payment_term,
          invoice_period_start,
          invoice_period_end,
          invoice_paid_at,
          invoice_paid_date
          from {{ ref('sde_harvest_projects_invoices')}}
)
  select * from sde_harvest_projects_invoices_ds
