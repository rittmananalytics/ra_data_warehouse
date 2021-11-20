{{config(enabled = target.type == 'bigquery')}}
{% if var("finance_warehouse_journal_sources") %}
{% if 'xero_accounting' in (var("finance_warehouse_journal_sources")) %}

{% if var("stg_xero_accounting_etl") == 'fivetran' %}

with journals AS (
    SELECT *
    FROM {{ source('fivetran_xero_accounting','journal') }}
),
journal_lines AS (
    SELECT *
    FROM {{ source('fivetran_xero_accounting','journal_line') }}
),
accounts AS (
    SELECT *
    FROM {{ source('fivetran_xero_accounting','account') }}
),
renamed AS (
  SELECT
    CONCAT('{{ var('stg_xero_accounting_id-prefix') }}',j.journal_id) AS journal_id,
    journal_date,
    journal_number,
    reference,
    source_id,
    source_type,
    l.account_code,
    a.account_id AS account_id,
    l.account_name,
    l.account_type,
    description,
    gross_amount,
    CONCAT('{{ var('stg_xero_accounting_id-prefix') }}',journal_line_id) AS journal_line_id,
    net_amount,
    tax_amount,
    tax_name,
    tax_type
FROM
   journals j
JOIN
   journal_lines l
ON
  j.journal_id = l.journal_id
LEFT JOIN
   accounts a
ON l.account_code = a.account_code)
SELECT *
FROM renamed

{% endif %}

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
