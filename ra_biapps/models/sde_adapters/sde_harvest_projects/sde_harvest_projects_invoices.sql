with source_harvest_base_invoices as (
SELECT
    *
FROM (
    SELECT
        *,
         MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_sdc_batched_at,
    CASE WHEN DATE_DIFF(DATE(due_date),DATE(paid_at),DAY) <=0 THEN true ELSE false END AS was_paid_ontime

    FROM
        {{ source('harvest_projects', 'invoices') }}
    )
    WHERE
        _sdc_batched_at = latest_sdc_batched_at
    ),
source_harvest_invoice_line_items as (
      SELECT
          *
      FROM (
          SELECT
              *,
               MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_sdc_batched_at
          FROM
              {{ source('harvest_projects', 'invoice_line_items') }}
          )
      WHERE
          _sdc_batched_at = latest_sdc_batched_at
    ),
source_harvest_expenses as (
  SELECT
      *
  FROM (
      SELECT
          *,
           MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_sdc_batched_at
      FROM
          {{ source('harvest_projects', 'expenses') }}
      )
  WHERE
      _sdc_batched_at = latest_sdc_batched_at
    ),
source_companies_pre_merged as
(
  select company_id, harvest_company_id
  from {{ ref('sde_companies_pre_merged') }}
  where harvest_company_id is not null
),
stg_harvest_invoices as (
select i.*,
  pm.company_id as company_id,
  e.total_rechargeable_expenses,
  row_number() over (partition by i.client_id order by i.created_at) as client_invoice_seq_no,
  date_diff(date(i.created_at),date(first_value(i.created_at) over (partition by i.client_id order by i.created_at)),MONTH) as months_since_first_invoice,
  date_diff(date(i.created_at),date(first_value(i.created_at) over (partition by i.client_id order by i.created_at)),QUARTER) as quarters_since_first_invoice,
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
from source_harvest_base_invoices i
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
    FROM source_harvest_invoice_line_items
group by 1,2,3,4,6,7,8,9)) a
on   i.id = a.invoice_id
left outer join (select invoice_id, sum(total_cost) as total_rechargeable_expenses FROM source_harvest_expenses  where billable group by 1 ) e
on i.id = e.invoice_id
join source_companies_pre_merged pm on i.client_id = pm.harvest_company_id
),
renamed as (
select 'harvest_projects' as source,
        company_id,
        id as invoice_id,
        number as invoice_number,
        project_id as invoice_project_id,
        client_id as invoice_company_id,
        subject as invoice_subject,
        revenue_amount_billed as invoice_local_total_revenue_amount,
        currency as invoice_currency,
        amount as total_local_amount,
        total_amount_billed as invoice_local_total_amount,
        services_amount_billed as invoice_local_total_service_amount,
        license_referral_fee_amount_billed as invoice_local_total_licence_referral_fee_amount,
        expenses_amount_billed as invoice_local_total_expenses_amount,
        support_amount_billed as invoice_local_total_support_amount,
        tax_billed as invoice_local_total_tax_amount,
        issue_date as invoice_issue_date,
        due_date as invoice_due_date,
        sent_at as invoice_sent_at,
        created_at as invoice_created_at,
        concat('harvest-',creator_id) as invoice_creator_users_id,
        due_amount as invoice_local_total_due_amount,
        tax as invoice_tax,
        payment_term as invoice_payment_term,
        period_start as invoice_period_start,
        period_end as invoice_period_end,
        paid_at as invoice_paid_at,
        paid_date as invoice_paid_date
from stg_harvest_invoices)
SELECT
  *
FROM
  renamed
