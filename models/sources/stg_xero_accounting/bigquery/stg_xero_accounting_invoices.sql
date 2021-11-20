{{config(enabled = target.type == 'bigquery')}}
{% if var("finance_warehouse_invoice_sources") %}
{% if 'xero_accounting' in var("finance_warehouse_invoice_sources") %}

{% if var("stg_xero_accounting_etl") == 'fivetran' %}


 with source AS (
   SELECT *
   FROM {{ source('fivetran_xero_accounting','invoice') }}
 ),
renamed AS (
     SELECT
       CONCAT('{{ var('stg_xero_accounting_id-prefix') }}',invoice_number) AS invoice_number,
       CONCAT('{{ var('stg_xero_accounting_id-prefix') }}',contact_id) AS company_id,
       CONCAT('{{ var('stg_xero_accounting_id-prefix') }}',invoice_id) AS invoice_id,
       CAST(null AS {{ dbt_utils.type_string() }}) AS project_id,
       CAST(null AS {{ dbt_utils.type_string() }}) AS invoice_creator_users_id,
       CAST(null AS {{ dbt_utils.type_string() }}) AS invoice_subject,
       CAST(date AS {{ dbt_utils.type_timestamp() }}) AS invoice_created_at_ts,
        CAST(null AS {{ dbt_utils.type_timestamp() }}) AS invoice_issue_at_ts,
       timestamp(due_date) AS invoice_due_at_ts,
        CAST(null AS {{ dbt_utils.type_timestamp() }}) AS invoice_sent_at_ts,
       timestamp(fully_paid_on_date) AS invoice_paid_at_ts,
        CAST(null AS {{ dbt_utils.type_timestamp() }}) AS invoice_period_start_at_ts,
        CAST(null AS {{ dbt_utils.type_timestamp() }}) AS invoice_period_end_at_ts,
       CAST(null AS {{ dbt_utils.type_numeric() }}) AS invoice_local_total_revenue_amount,
       CONCAT('{{ var('stg_xero_accounting_id-prefix') }}',currency_code) AS invoice_currency,
       total AS total_local_amount,
       CAST(null AS numeric) AS invoice_local_total_billed_amount,
       CAST(null AS numeric) AS invoice_local_total_services_amount,
       CAST(null AS numeric) AS invoice_local_total_licence_referral_fee_amount,
       CAST(null AS numeric) AS invoice_local_total_expenses_amount,
       CAST(null AS numeric) AS invoice_local_total_support_amount,
       CAST(null AS {{ dbt_utils.type_string() }}) AS invoice_tax_rate_pct,
       total_tax AS invoice_local_total_tax_amount,
       amount_due AS invoice_local_total_due_amount,
       CAST(null AS {{ dbt_utils.type_string() }}) AS invoice_payment_term,
       case when status = 'AUTHORISED' then 'Authorised'
            when status = 'PAID' then 'Paid'
            when status = 'VOIDED' then 'Voided'
            else status end AS invoice_status,
            case when type = 'ACCREC' then 'Xero - Sales'
                 when type = 'ACCPAY' then 'Xero - Purchases'
                 else CONCAT('Xero - ',type) end AS invoice_type
    FROM source)

{% endif %}

 SELECT
   *
 FROM
   renamed



   {% else %} {{config(enabled=false)}} {% endif %}
   {% else %} {{config(enabled=false)}} {% endif %}
