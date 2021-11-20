{% if var('finance_warehouse_invoice_sources') %}


with t_invoices_merge_list AS (

  {% for source in var('finance_warehouse_invoice_sources') %}
    {% set relation_source = 'stg_' + source + '_invoices' %}

    SELECT
      '{{source}}' AS source,
      *
      FROM {{ ref(relation_source) }}

      {% if not loop.last %}union all{% endif %}
    {% endfor %}

    ),
    all_invoice_ids AS (
           SELECT invoice_number, array_agg(distinct invoice_id ignore nulls) AS all_invoice_ids
           FROM t_invoices_merge_list
           group by 1),
       merged AS (
       SELECT invoice_number,
       max(company_id) AS company_id,
       {% if 'harvest_projects' in var("projects_warehouse_timesheet_sources") %}
       max(project_id) AS project_id,
       max(invoice_creator_users_id) AS invoice_creator_users_id,
       {% endif %}
       max(invoice_subject) AS invoice_subject,
       min(invoice_created_at_ts) AS invoice_created_at_ts,
       min(invoice_issue_at_ts) AS invoice_issue_at_ts,
       min(invoice_due_at_ts) AS invoice_due_at_ts,
       min(invoice_sent_at_ts) AS invoice_sent_at_ts,
       max(invoice_paid_at_ts) AS invoice_paid_at_ts,
       max(invoice_period_start_at_ts) AS invoice_period_start_at_ts,
       max(invoice_period_end_at_ts) AS invoice_period_end_at_ts,
       max(invoice_local_total_revenue_amount) AS invoice_local_total_revenue_amount,
       max(invoice_currency) AS invoice_currency,
       max(total_local_amount) AS total_local_amount,
       {% if 'harvest_projects' in var("projects_warehouse_timesheet_sources") %}
       max(invoice_local_total_billed_amount) AS invoice_local_total_billed_amount,
       max(invoice_local_total_services_amount) AS invoice_local_total_services_amount,
       max(invoice_local_total_licence_referral_fee_amount) AS invoice_local_total_licence_referral_fee_amount,
       max(invoice_local_total_expenses_amount) AS invoice_local_total_expenses_amount,
       max(invoice_local_total_support_amount) AS invoice_local_total_support_amount,
       {% endif %}
       max(invoice_tax_rate_pct) AS invoice_tax_rate_pct,
       max(invoice_local_total_tax_amount) AS invoice_local_total_tax_amount,
       max(invoice_local_total_due_amount) AS invoice_local_total_due_amount,
       max(invoice_payment_term) AS invoice_payment_term,
       max(invoice_status) AS invoice_status,
       max(invoice_type) AS invoice_type
       FROM t_invoices_merge_list
       group by 1),
    joined AS (
      SELECT i.*,
      a.all_invoice_ids,
      timestamp_diff(invoice_paid_at_ts,invoice_issue_at_ts,DAY) AS invoice_total_days_to_pay,
      30-timestamp_diff(invoice_paid_at_ts,invoice_issue_at_ts,DAY) AS invoice_total_days_variance_on_payment_terms,
      timestamp_diff(invoice_paid_at_ts,invoice_due_at_ts,DAY) AS invoice_total_days_overdue
      FROM  merged i
      join all_invoice_ids a on i.invoice_number = a.invoice_number
    )
SELECT
 *
FROM
 joined

 {% else %}

 {{config(enabled=false)}}

 {% endif %}
