with t_invoices_merge_list as (

      {% if var("enable_harvest_projects_source") %}
      SELECT *
      FROM   {{ ref('stg_harvest_projects_invoices') }}
      {% endif %}

      {% if var("enable_xero_accounting_source") and var("enable_harvest_projects_source")  %}
      UNION ALL
      {% endif %}

      {% if var("enable_xero_accounting_source")  %}
      SELECT *
      FROM   {{ ref('stg_xero_accounting_invoices') }}
      {% endif %}


    ),
    all_invoice_ids as (
           SELECT invoice_number, array_agg(distinct invoice_id ignore nulls) as all_invoice_ids
           FROM t_invoices_merge_list
           group by 1),
       merged as (
       SELECT invoice_number,
       max(company_id) as company_id,
       {% if var("enable_harvest_projects_source") %}
       max(project_id) as project_id,
       max(invoice_creator_users_id) as invoice_creator_users_id,
       {% endif %}
       max(invoice_subject) as invoice_subject,
       min(invoice_created_at_ts) as invoice_created_at_ts,
       min(invoice_issue_at_ts) as invoice_issue_at_ts,
       min(invoice_due_at_ts) as invoice_due_at_ts,
       min(invoice_sent_at_ts) as invoice_sent_at_ts,
       max(invoice_paid_at_ts) as invoice_paid_at_ts,
       max(invoice_period_start_at_ts) as invoice_period_start_at_ts,
       max(invoice_period_end_at_ts) as invoice_period_end_at_ts,
       max(invoice_local_total_revenue_amount) as invoice_local_total_revenue_amount,
       max(invoice_currency) as invoice_currency,
       max(total_local_amount) as total_local_amount,
       {% if var("enable_harvest_projects_source") %}
       max(invoice_local_total_billed_amount) as invoice_local_total_billed_amount,
       max(invoice_local_total_services_amount) as invoice_local_total_services_amount,
       max(invoice_local_total_licence_referral_fee_amount) as invoice_local_total_licence_referral_fee_amount,
       max(invoice_local_total_expenses_amount) as invoice_local_total_expenses_amount,
       max(invoice_local_total_support_amount) as invoice_local_total_support_amount,
       {% endif %}
       max(invoice_tax_rate_pct) as invoice_tax_rate_pct,
       max(invoice_local_total_tax_amount) as invoice_local_total_tax_amount,
       max(invoice_local_total_due_amount) as invoice_local_total_due_amount,
       max(invoice_payment_term) as invoice_payment_term,
       max(invoice_status) as invoice_status,
       max(invoice_type) as invoice_type
       from t_invoices_merge_list
       group by 1),
    joined as (
      SELECT i.*,
      a.all_invoice_ids,
      timestamp_diff(invoice_paid_at_ts,invoice_issue_at_ts,DAY) as invoice_total_days_to_pay,
      30-timestamp_diff(invoice_paid_at_ts,invoice_issue_at_ts,DAY) as invoice_total_days_variance_on_payment_terms,
      timestamp_diff(invoice_paid_at_ts,invoice_due_at_ts,DAY) as invoice_total_days_overdue
      FROM  merged i
      join all_invoice_ids a on i.invoice_number = a.invoice_number
    )
SELECT
 *
FROM
 joined
