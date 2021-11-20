{% if var("finance_warehouse_journal_sources") %}

{{
    config(
        unique_key='journal_pk',
        alias='general_ledger_fact'
    )
}}

with journals AS (

    SELECT *
    FROM {{ ref('wh_journals_fact')}}

)

, accounts AS (

    SELECT *
    FROM {{ ref('wh_chart_of_accounts_dim')}}

), invoices AS (

    SELECT i.*,
           all_invoice_ids AS invoice_id
    FROM {{ ref('wh_invoices_fact')}} i ,
           unnest(all_invoice_ids) all_invoice_ids

), bank_transactions AS (

    SELECT *
    FROM {{ ref('wh_bank_transactions_fact')}}

), companies AS (

    SELECT *
    FROM {{ ref('wh_companies_dim')}}

), joined AS (

    SELECT
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

        case when journals.source_type in ('ACCPAY', 'ACCREC') then CONCAT('xero-',journals.source_id) end AS invoice_id,
        case when journals.source_type in ('CASHREC','CASHPAID') then CONCAT('xero-',journals.source_id) end AS bank_transaction_id,
        case when journals.source_type in ('TRANSFER') then CONCAT('xero-',journals.source_id) end AS bank_transfer_id,
        case when journals.source_type in ('MANJOURNAL') then CONCAT('xero-',journals.source_id) end AS manual_journal_id,
        case when journals.source_type in ('APPREPAYMENT', 'APOVERPAYMENT', 'ACCPAYPAYMENT', 'ACCRECPAYMENT', 'ARCREDITPAYMENT', 'APCREDITPAYMENT') then CONCAT('xero-',journals.source_id) end AS payment_id,
        case when journals.source_type in ('ACCPAYCREDIT','ACCRECCREDIT') then CONCAT('xero-',journals.source_id) end AS credit_note_id

    FROM journals
    left join accounts
        on accounts.account_id = journals.account_id

), first_company AS (

    SELECT
        joined.*,
        coalesce(
            invoices.company_pk,
            bank_transactions.contact_id
        ) AS company_pk
    FROM joined
    left join invoices
        using (invoice_id)
    left join bank_transactions
        using (bank_transaction_id)


), second_company AS (

    SELECT
        first_company.*,
        companies.company_name
    FROM first_company
    left join companies
        using (company_pk)

)

SELECT *
FROM joined


{% else %} {{config(enabled=false)}} {% endif %}
