{{config(enabled = target.type == 'bigquery')}}
{% if var("finance_warehouse_invoice_sources") %}
{% if 'xero_accounting' in var("finance_warehouse_invoice_sources") %}

{% if var("stg_xero_accounting_etl") == 'stitch' %}

WITH
  source as (
    {{ filter_stitch_relation(relation=var('stg_xero_accounting_stitch_invoices_table'),unique_column='invoiceid') }}

      ),
renamed as (
  SELECT
    concat('{{ var('stg_xero_accounting_id-prefix') }}',invoicenumber) as invoice_number,
    concat('{{ var('stg_xero_accounting_id-prefix') }}',contact.contactid) as company_id,
    concat('{{ var('stg_xero_accounting_id-prefix') }}',invoiceid) as invoice_id,
    cast(null as string) as project_id,
    cast(null as string) as invoice_creator_users_id,
    cast(null as string) as invoice_subject,
    date as invoice_created_at_ts,
    cast(null as timestamp) as invoice_issue_at_ts,
    duedatestring as invoice_due_at_ts,
    cast(null as timestamp) as invoice_sent_at_ts,
    fullypaidondate as invoice_paid_at_ts,
    cast(null as timestamp) as invoice_period_start_at_ts,
    cast(null as timestamp) as invoice_period_end_at_ts,
    cast(null as numeric) as invoice_local_total_revenue_amount,
    concat('{{ var('stg_xero_accounting_id-prefix') }}',currencycode) as invoice_currency,
    total as total_local_amount,
    cast(null as numeric) as invoice_local_total_billed_amount,
    cast(null as numeric) as invoice_local_total_services_amount,
    cast(null as numeric) as invoice_local_total_licence_referral_fee_amount,
    cast(null as numeric) as invoice_local_total_expenses_amount,
    cast(null as numeric) as invoice_local_total_support_amount,
    cast(null as string) as invoice_tax_rate_pct,
    totaltax as invoice_local_total_tax_amount,
    amountdue as invoice_local_total_due_amount,
    cast (null as string) as invoice_payment_term,
    case when status = 'AUTHORISED' then 'Authorised'
         when status = 'PAID' then 'Paid'
         when status = 'VOIDED' then 'Voided'
         else status end as invoice_status,
         case when type = 'ACCREC' then 'Xero - Sales'
              when type = 'ACCPAY' then 'Xero - Purchases'
              else concat('Xero - ',type) end as invoice_type
 FROM source)

 {% elif var("stg_xero_accounting_etl") == 'fivetran' %}

 with source as (
   select *
   from {{ var('stg_xero_accounting_fivetran_invoices_table') }}
 ),
renamed as (
     SELECT
       concat('{{ var('stg_xero_accounting_id-prefix') }}',invoice_number) as invoice_number,
       concat('{{ var('stg_xero_accounting_id-prefix') }}',contact_id) as company_id,
       concat('{{ var('stg_xero_accounting_id-prefix') }}',invoice_id) as invoice_id,
       cast(null as string) as project_id,
       cast(null as string) as invoice_creator_users_id,
       cast(null as string) as invoice_subject,
       timestamp(date) as invoice_created_at_ts,
       cast(null as timestamp) as invoice_issue_at_ts,
       timestamp(due_date) as invoice_due_at_ts,
       cast(null as timestamp) as invoice_sent_at_ts,
       timestamp(fully_paid_on_date) as invoice_paid_at_ts,
       cast(null as timestamp) as invoice_period_start_at_ts,
       cast(null as timestamp) as invoice_period_end_at_ts,
       cast(null as numeric) as invoice_local_total_revenue_amount,
       concat('{{ var('stg_xero_accounting_id-prefix') }}',currency_code) as invoice_currency,
       total as total_local_amount,
       cast(null as numeric) as invoice_local_total_billed_amount,
       cast(null as numeric) as invoice_local_total_services_amount,
       cast(null as numeric) as invoice_local_total_licence_referral_fee_amount,
       cast(null as numeric) as invoice_local_total_expenses_amount,
       cast(null as numeric) as invoice_local_total_support_amount,
       cast(null as string) as invoice_tax_rate_pct,
       total_tax as invoice_local_total_tax_amount,
       amount_due as invoice_local_total_due_amount,
       cast (null as string) as invoice_payment_term,
       case when status = 'AUTHORISED' then 'Authorised'
            when status = 'PAID' then 'Paid'
            when status = 'VOIDED' then 'Voided'
            else status end as invoice_status,
            case when type = 'ACCREC' then 'Xero - Sales'
                 when type = 'ACCPAY' then 'Xero - Purchases'
                 else concat('Xero - ',type) end as invoice_type
    FROM source)

{% endif %}

 SELECT
   *
 FROM
   renamed



   {% else %} {{config(enabled=false)}} {% endif %}
   {% else %} {{config(enabled=false)}} {% endif %}
