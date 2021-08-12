{% if var("finance_warehouse_journal_sources") %}

{{
    config(
        unique_key='journal_pk',
        alias='general_ledger_fact'
    )
}}

with journals as (

    select *
    from {{ ref('wh_journals_fact')}}

)

, accounts as (

    select *
    from {{ ref('wh_chart_of_accounts_dim')}}

), invoices as (

    select i.*,
           all_invoice_ids as invoice_id
    from {{ ref('wh_invoices_fact')}} i ,
           unnest(all_invoice_ids) all_invoice_ids

), bank_transactions as (

    select *
    from {{ ref('wh_bank_transactions_fact')}}

), companies as (

    select *
    from {{ ref('wh_companies_dim')}}

), joined as (

    select
        journals.journal_pk,
        journals.journal_id,
        journals.journal_date,
        journals.journal_number,
        journals.reference,
        journals.source_id,
        journals.source_type,

        journals.journal_line_id,
        journals.account_code,
        journals.account_id,
        journals.account_name,
        journals.account_type,
        journals.description,
        journals.gross_amount,
        journals.net_amount,
        journals.tax_amount,
        journals.tax_name,
        journals.tax_type,

        accounts.account_class,

        case when journals.source_type in ('ACCPAY', 'ACCREC') then concat('xero-',journals.source_id) end as invoice_id,
        case when journals.source_type in ('CASHREC','CASHPAID') then concat('xero-',journals.source_id) end as bank_transaction_id,
        case when journals.source_type in ('TRANSFER') then concat('xero-',journals.source_id) end as bank_transfer_id,
        case when journals.source_type in ('MANJOURNAL') then concat('xero-',journals.source_id) end as manual_journal_id,
        case when journals.source_type in ('APPREPAYMENT', 'APOVERPAYMENT', 'ACCPAYPAYMENT', 'ACCRECPAYMENT', 'ARCREDITPAYMENT', 'APCREDITPAYMENT') then concat('xero-',journals.source_id) end as payment_id,
        case when journals.source_type in ('ACCPAYCREDIT','ACCRECCREDIT') then concat('xero-',journals.source_id) end as credit_note_id

    from journals
    left join accounts
        on accounts.account_id = journals.account_id

), first_company as (

    select
        joined.*,
        coalesce(
            invoices.company_pk,
            bank_transactions.contact_id
        ) as company_pk
    from joined
    left join invoices
        using (invoice_id)
    left join bank_transactions
        using (bank_transaction_id)


), second_company as (

    select
        first_company.*,
        companies.company_name
    from first_company
    left join companies
        using (company_pk)

)

select *
from joined


{% else %} {{config(enabled=false)}} {% endif %}
