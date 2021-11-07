{% if var("finance_warehouse_journal_sources") %}

{{
    config(
        alias='journals_fact',
        unique_key='journal_pk',
        materialized='table'
    )
}}


WITH journals AS
  (
  SELECT *
  FROM   {{ ref('int_journals') }}
  )

SELECT
   {{ dbt_utils.surrogate_key(['journal_id','journal_line_id']) }}  as journal_pk,
   *
FROM
   journals

   {% else %} {{config(enabled=false)}} {% endif %}
