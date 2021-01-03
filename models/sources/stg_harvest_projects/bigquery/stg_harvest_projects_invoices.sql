{{config(enabled = target.type == 'bigquery')}}
{% if var("finance_warehouse_invoice_sources") %}
{% if 'harvest_projects' in var("finance_warehouse_invoice_sources") %}

with source as (
  {{ filter_stitch_relation(relation=var('stg_harvest_projects_stitch_invoices_table'),unique_column='id') }}
    ),
harvest_invoice_line_items as (
  {{ filter_stitch_relation(relation=var('stg_harvest_projects_stitch_invoice_line_items_table'),unique_column='id') }}
    ),
harvest_expenses as (
  {{ filter_stitch_relation(relation=var('stg_harvest_projects_stitch_expenses_table'),unique_column='id') }}
    ),
joined as (
select i.*,
  client_id as company_id,
  id as invoice_id,
  e.total_rechargeable_expenses,
  row_number() over (partition by i.client_id order by i.created_at) as client_invoice_seq_no,
  {{ dbt_utils.datediff('date(first_value(i.created_at) over (partition by i.client_id order by i.created_at))','date(i.created_at)','MONTH') }} as months_since_first_invoice,
  {{ dbt_utils.datediff('date(first_value(i.created_at) over (partition by i.client_id order by i.created_at))','date(i.created_at)','QUARTER') }} as quarters_since_first_invoice,
  amount - ifnull(cast(tax_amount as float64),0) - ifnull(cast(e.total_rechargeable_expenses as float64),0) as net_amount,
  ifnull(a.total_amount_billed,0) as total_amount_billed,
  ifnull(a.services_amount_billed,0) as services_amount_billed,
  ifnull(a.license_referral_fee_amount_billed,0) as license_referral_fee_amount_billed,
  ifnull(a.expenses_amount_billed,0) as expenses_amount_billed,
  ifnull(a.support_amount_billed,0) as support_amount_billed,
  ifnull(a.tax_billed,0) as tax_billed,
  ifnull(a.services_amount_billed,0) + ifnull(a.license_referral_fee_amount_billed,0) + ifnull(a.support_amount_billed,0) as revenue_amount_billed,
  project_id,
  invoice_line_item_id
from source i
join (select *,
       case when taxed then total_amount_billed *.2 end as tax_billed
       from (
         SELECT invoice_id,
         project_id,
         id as invoice_line_item_id,
         taxed,
         sum(amount) as total_amount_billed,
         ifnull((case when kind = 'Service' then amount end),0) as services_amount_billed,
         ifnull((case when kind = 'License Referral Fee' then amount end),0) as license_referral_fee_amount_billed,
         ifnull((case when kind = 'Product' then amount end),0) as expenses_amount_billed,
         ifnull((case when kind = 'Support' then amount end),0) as support_amount_billed
    FROM harvest_invoice_line_items
group by 1,2,3,4,6,7,8,9 )) a
on   i.id = a.invoice_id
left outer join (select invoice_id, sum(total_cost) as total_rechargeable_expenses FROM harvest_expenses  where billable group by 1 ) e
on i.id = e.invoice_id
),
renamed as (
select  concat('{{ var('stg_harvest_projects_id-prefix') }}',number) as invoice_number,
        concat('{{ var('stg_harvest_projects_id-prefix') }}',company_id) as company_id,
        concat('{{ var('stg_harvest_projects_id-prefix') }}',invoice_id) as invoice_id,
        concat('{{ var('stg_harvest_projects_id-prefix') }}',project_id) as project_id,
        concat('{{ var('stg_harvest_projects_id-prefix') }}',creator_id) as invoice_creator_users_id,
        subject as invoice_subject,
        created_at as invoice_created_at_ts,
        issue_date as invoice_issue_at_ts,
        due_date as invoice_due_at_ts,
        sent_at as invoice_sent_at_ts,
        paid_at as invoice_paid_at_ts,
        period_start as invoice_period_start_at_ts,
        period_end as invoice_period_end_at_ts,
        revenue_amount_billed as invoice_local_total_revenue_amount,
        currency as invoice_currency,
        amount as total_local_amount,
        total_amount_billed as invoice_local_total_billed_amount,
        services_amount_billed as invoice_local_total_services_amount,
        license_referral_fee_amount_billed as invoice_local_total_licence_referral_fee_amount,
        expenses_amount_billed as invoice_local_total_expenses_amount,
        support_amount_billed as invoice_local_total_support_amount,
        tax as invoice_tax_rate_pct,
        tax_billed as invoice_local_total_tax_amount,
        due_amount as invoice_local_total_due_amount,
        payment_term as invoice_payment_term,
        case when state = 'open' then 'Open'
             when state = 'paid' then 'Paid'
             when state = 'draft' then 'Draft'
             else 'Other' end as invoice_status,
        'Harvest - Client Billing' as invoice_type,

from joined)
SELECT
  *
FROM
  renamed

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
