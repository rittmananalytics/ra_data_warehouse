{% if not enable_xero_accounting %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH
  invoices as
    (
      SELECT
        *
      FROM (
        SELECT
          *,
          MAX(_sdc_batched_at) OVER (PARTITION BY invoiceid ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_sdc_batched_at
        FROM
          {{ source('xero_accounting', 'invoices') }})
      WHERE
        latest_sdc_batched_at = _sdc_batched_at
      )
  SELECT
  'xero_accounting'       source,
    invoiceid as invoice_id,
    invoicenumber as invoice_number,
    reference as invoice_reference,
    plannedpaymentdate as invoice_planned_payment_date,
    case when status = 'AUTHORISED' then 'Authorised'
         when status = 'PAID' then 'Paid'
         when status = 'VOIDED' then 'Voided'
         else status end as invoice_status,
    case when type = 'ACCREC' then 'Accounts Payable'
         when type = 'ACCPAY' then 'Accounts Payable'
         else type end as invoice_type,
    haserrors as invoice_has_errors,
    duedatestring as invoice_due_date,
    url as invoice_url,
    senttocontact as invoice_sent_to_contact,
    isdiscounted as invoice_is_discounted,
    date as invoice_date,
    fullypaidondate as invoice_fully_paid_on_date,
    lineamounttypes as invoice_line_amount_types,
    total as invoice_local_total_amount,
    totaltax as invoice_local_total_tax_amount,
    amountpaid as invoice_local_total_paid_amount,
    amountdue as invoice_local_total_due_amount,
    currencycode as invoice_currency_code,
    subtotal as invoice_local_total_subtotal_amount,
    amountcredited as invoice_local_total_credited_amount,
    currencyrate as invoice_currency_rate,
    contact.contactid as invoice_contact_id
 FROM invoices
