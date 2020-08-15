{% if not var("enable_stripe_subscriptions_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
{% if var("etl") == 'segment' %}
with source as (
  {{ filter_segment_table(var('stg_stripe_payments_segment_schema'),var('stg_stripe_payments_segment_plans_table')) }}
),
renamed as (
SELECT
    concat('{{ var('stg_stripe_payments_id-prefix') }}',id) as plan_id,
    name as plan_name,
    `interval` as plan_interval,
    interval_count as plan_interval_count,
    statement_descriptor as plan_statement_descriptor,
    amount as plan_amount,
    currency as plan_currency,
    is_deleted as plan_is_deleted,
    created   as plan_created_ts,
    cast(null as timestamp) as plan_last_modified_ts
FROM
  source
)
select * from renamed
{% endif %}
