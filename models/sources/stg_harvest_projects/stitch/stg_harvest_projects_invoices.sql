{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("finance_warehouse_invoice_sources") %}
{% if 'harvest_projects' in var("finance_warehouse_invoice_sources") %}

with source AS (
  {{ filter_stitch_relation(relation=source('stitch_harvest_projects', 'invoices'),unique_column='id') }}
    ),
harvest_invoice_line_items AS (
  {{ filter_stitch_relation(relation=source('stitch_harvest_projects', 'invoice_line_items'),unique_column='id') }}
    ),
harvest_expenses AS (
  {{ filter_stitch_relation(relation=source('stitch_harvest_projects', 'expenses'),unique_column='id') }}
    ),
joined AS (
SELECT i.*,
  client_id AS company_id,
  id AS invoice_id,
  e.total_rechargeable_expenses,
  row_number() over (PARTITION BYi.client_id order by i.created_at) AS client_invoice_seq_no,
  {{ dbt_utils.datediff('date(first_value(i.created_at) over (PARTITION BYi.client_id order by i.created_at))','date(i.created_at)','MONTH') }} AS months_since_first_invoice,
  {{ dbt_utils.datediff('date(first_value(i.created_at) over (PARTITION BYi.client_id order by i.created_at))','date(i.created_at)','QUARTER') }} AS quarters_since_first_invoice,
  amount - IFNULL(CAST(tax_amount AS float64),0) - IFNULL(CAST(e.total_rechargeable_expenses AS float64),0) AS net_amount,
  IFNULL(a.total_amount_billed,0) AS total_amount_billed,
  IFNULL(a.services_amount_billed,0) AS services_amount_billed,
  IFNULL(a.license_referral_fee_amount_billed,0) AS license_referral_fee_amount_billed,
  IFNULL(a.expenses_amount_billed,0) AS expenses_amount_billed,
  IFNULL(a.support_amount_billed,0) AS support_amount_billed,
  IFNULL(a.tax_billed,0) AS tax_billed,
  IFNULL(a.services_amount_billed,0) + IFNULL(a.license_referral_fee_amount_billed,0) + IFNULL(a.support_amount_billed,0) AS revenue_amount_billed,
  project_id,
  invoice_line_item_id
FROM source i
join (SELECT *,
       case when taxed then total_amount_billed *.2 end AS tax_billed
       FROM (
         SELECT invoice_id,
         project_id,
         id AS invoice_line_item_id,
         taxed,
         sum(amount) AS total_amount_billed,
         IFNULL((case when kind = 'Service' then amount end),0) AS services_amount_billed,
         IFNULL((case when kind = 'License Referral Fee' then amount end),0) AS license_referral_fee_amount_billed,
         IFNULL((case when kind = 'Product' then amount end),0) AS expenses_amount_billed,
         IFNULL((case when kind = 'Support' then amount end),0) AS support_amount_billed
    FROM harvest_invoice_line_items
group by 1,2,3,4,6,7,8,9 )) a
on   i.id = a.invoice_id
left outer join (SELECT invoice_id, sum(total_cost) AS total_rechargeable_expenses FROM harvest_expenses  where billable group by 1 ) e
on i.id = e.invoice_id
),
renamed AS (
SELECT  CONCAT('{{ var('stg_harvest_projects_id-prefix') }}',number) AS invoice_number,
        CONCAT('{{ var('stg_harvest_projects_id-prefix') }}',company_id) AS company_id,
        CONCAT('{{ var('stg_harvest_projects_id-prefix') }}',invoice_id) AS invoice_id,
        CONCAT('{{ var('stg_harvest_projects_id-prefix') }}',project_id) AS project_id,
        CONCAT('{{ var('stg_harvest_projects_id-prefix') }}',creator_id) AS invoice_creator_users_id,
        subject AS invoice_subject,
        created_at AS invoice_created_at_ts,
        issue_date AS invoice_issue_at_ts,
        due_date AS invoice_due_at_ts,
        sent_at AS invoice_sent_at_ts,
        paid_at AS invoice_paid_at_ts,
        period_start AS invoice_period_start_at_ts,
        period_end AS invoice_period_end_at_ts,
        revenue_amount_billed AS invoice_local_total_revenue_amount,
        currency AS invoice_currency,
        amount AS total_local_amount,
        total_amount_billed AS invoice_local_total_billed_amount,
        services_amount_billed AS invoice_local_total_services_amount,
        license_referral_fee_amount_billed AS invoice_local_total_licence_referral_fee_amount,
        expenses_amount_billed AS invoice_local_total_expenses_amount,
        support_amount_billed AS invoice_local_total_support_amount,
        tax AS invoice_tax_rate_pct,
        tax_billed AS invoice_local_total_tax_amount,
        due_amount AS invoice_local_total_due_amount,
        payment_term AS invoice_payment_term,
        case when state = 'open' then 'Open'
             when state = 'paid' then 'Paid'
             when state = 'draft' then 'Draft'
             else 'Other' end AS invoice_status,
        'Harvest - Client Billing' AS invoice_type,

FROM joined)
SELECT
  *
FROM
  renamed

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
