{% if var("finance_warehouse_journal_sources") %}

{{
    config(
        unique_key='profit_and_loss_pk',
        alias='profit_and_loss_report_fact'
    )
}}
with spine as (

    {{
        dbt_utils.date_spine(
            datepart="month",
            start_date="cast('2019-01-01' as date)",
            end_date=dbt_utils.dateadd(datepart='month', interval=1, from_date_or_timestamp="current_date")
        )
    }}

), cleaned as (

    select cast(date_month as date) as date_month
    from spine

),
calendar as (
  select *
  from cleaned)
, ledger as (

    select *
    from {{ ref('wh_general_ledger_fact') }}

), joined as (

    select
        {{ dbt_utils.surrogate_key(['calendar.date_month','ledger.account_id']) }} as profit_and_loss_pk,
        calendar.date_month,
        ledger.account_id,
        ledger.account_name,
        ledger.account_code,
        ledger.account_type,
        ledger.account_class,
        coalesce(sum(ledger.net_amount * -1),0) as net_amount
    from calendar
    left join ledger
        on calendar.date_month = cast({{ dbt_utils.date_trunc('month', 'ledger.journal_date') }} as date)
    where ledger.account_class in ('REVENUE','EXPENSE')
    {{ dbt_utils.group_by(7) }}

)

select *
from joined

{% else %} {{config(enabled=false)}} {% endif %}
