{{config(enabled = target.type == 'bigquery')}}
{% if var("subscriptions_warehouse_sources") %}
{% if 'stripe_subscriptions' in var("subscriptions_warehouse_sources") %}

{% if var("etl") == 'segment' %}
with source as (
  {{ filter_segment_relation(relation=source('segment_stripe_subscriptions','plans')) }}
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
     cast(null as {{ dbt_utils.type_timestamp() }}) as plan_last_modified_ts
FROM
  source
)
select * from renamed
{% endif %}

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
