{{config(enabled = target.type == 'bigquery')}}
{% if var("subscriptions_warehouse_sources") %}
{% if 'stripe_subscriptions' in var("subscriptions_warehouse_sources") %}

{% if var("etl") == 'segment' %}
with source AS (
  {{ filter_segment_relation(relation=source('segment_stripe_subscriptions','plans')) }}
),
renamed AS (
SELECT
    CONCAT('{{ var('stg_stripe_payments_id-prefix') }}',id) AS plan_id,
    name AS plan_name,
    `interval` AS plan_interval,
    interval_count AS plan_interval_count,
    statement_descriptor AS plan_statement_descriptor,
    amount AS plan_amount,
    currency AS plan_currency,
    is_deleted AS plan_is_deleted,
    created   AS plan_created_ts,
     CAST(null AS {{ dbt_utils.type_timestamp() }}) AS plan_last_modified_ts
FROM
  source
)
SELECT * FROM renamed
{% endif %}

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
