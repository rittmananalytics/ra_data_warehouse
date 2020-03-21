{{
    config(
        materialized='table'
    )
}}
with harvest_invoices as (
SELECT
    *
FROM (
    SELECT
        *,
         MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_sdc_batched_at,
    CASE WHEN DATE_DIFF(DATE(due_date),DATE(paid_at),DAY) <=0 THEN true ELSE false END AS was_paid_ontime

    FROM
        {{ source('harvest', 'invoices') }}
    ),
harvest_invoice_line_items as (
      SELECT
          *
      FROM (
          SELECT
              *,
               MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_sdc_batched_at
          FROM
              {{ source('harvest', 'invoice_line_items') }}
          )
      WHERE
          _sdc_batched_at = latest_sdc_batched_at
    )
WHERE
    _sdc_batched_at = latest_sdc_batched_at),
harvest_expenses as (
  SELECT
      *
  FROM (
      SELECT
          *,
           MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_sdc_batched_at
      FROM
          {{ source('harvest', 'expenses') }}
      )
  WHERE
      _sdc_batched_at = latest_sdc_batched_at
)
select i.*, e.total_rechargeable_expenses,
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
from harvest_invoices i
join (select *,
       case when taxed then total_amount_billed *.2 end as tax_billed
       from (
SELECT invoice_id,
project_id,
id as invoice_line_item_id,
       taxed,
       sum(amount) as total_amount_billed,
       ifnull(sum(case when kind = 'Service' then amount end),0) as services_amount_billed,
       ifnull(sum(case when kind = 'License Referral Fee' then amount end),0) as license_referral_fee_amount_billed,
       ifnull(sum(case when kind = 'Product' then amount end),0) as expenses_amount_billed,
       ifnull(sum(case when kind = 'Support' then amount end),0) as support_amount_billed
FROM harvest_invoice_line_items
group by 1,2,3,4)) a
on   i.id = a.invoice_id
left outer join (select invoice_id, sum(total_cost) as total_rechargeable_expenses FROM harvest_expenses  where billable group by 1 ) e
on i.id = e.invoice_id
