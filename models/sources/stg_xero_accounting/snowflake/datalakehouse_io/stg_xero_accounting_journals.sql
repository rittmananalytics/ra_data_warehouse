{{config(enabled = target.type == 'snowflake')}}
{% if var("finance_warehouse_journal_sources") %}
{% if 'xero_accounting' in (var("finance_warehouse_journal_sources")) %}

{% if var("stg_xero_accounting_etl") == 'datalakehouse_io' %}

with journals as (
    select *
    from {{ source('datalakehouse_xero_accounting','journal') }}
),
journal_lines as (
    select *
    from {{ source('datalakehouse_xero_accounting','journal_line') }}
),
accounts as (
    select *
    from {{ source('datalakehouse_xero_accounting','account') }}
),
renamed as (
  SELECT
    concat('{{ var('stg_xero_accounting_id-prefix') }}',j.journal_id) as journal_id,
    j.journal_date,
    j.journal_number,
    j.reference,
    j.source_id,
    j.source_type,
    l.account_code,
    a.account_id as account_id,
    l.account_name,
    l.account_type,
    l.description,
    l.gross_amount,
    concat('{{ var('stg_xero_accounting_id-prefix') }}',l.journal_line_id) as journal_line_id,
    l.net_amount,
    l.tax_amount,
    l.tax_name,
    l.tax_type
FROM
   journals j
JOIN
   journal_lines l
ON
  j.journal_id = l.journal_id
LEFT JOIN
   accounts a
ON l.account_code = a.code)
select *
from renamed

{% endif %}

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
