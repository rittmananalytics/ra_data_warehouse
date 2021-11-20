{% if var("finance_warehouse_journal_sources") %}

{{
    config(
        unique_key='profit_and_loss_pk',
        alias='profit_and_loss_report_fact'
    )
}}
with spine AS (

    {{
        dbt_utils.date_spine(
            datepart="month",
            start_date="CAST('2019-01-01' AS date)",
            end_date=dbt_utils.dateadd(datepart='month', interval=1, from_date_or_timestamp="current_date")
        )
    }}

), cleaned AS (

    SELECT CAST(date_month AS date) AS date_month
    FROM spine

),
calendar AS (
  SELECT *
  FROM cleaned)
, ledger AS (

    SELECT *
    FROM {{ ref('wh_general_ledger_fact') }}

), joined AS (

    SELECT
        {{ dbt_utils.surrogate_key(['calendar.date_month','ledger.account_id']) }} AS profit_and_loss_pk,
        calendar.date_month,
        ledger.account_id,
        ledger.account_name,
        ledger.account_code,
        ledger.account_type,
        ledger.account_class,
        coalesce(sum(ledger.net_amount * -1),0) AS net_amount
    FROM calendar
    left join ledger
        on calendar.date_month = CAST({{ dbt_utils.date_trunc('month', 'ledger.journal_date') }} AS date)
    where ledger.account_class in ('REVENUE','EXPENSE')
    {{ dbt_utils.group_by(7) }}

)

SELECT *
FROM joined

{% else %} {{config(enabled=false)}} {% endif %}
