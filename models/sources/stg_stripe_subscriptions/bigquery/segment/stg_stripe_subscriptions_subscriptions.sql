{{config(enabled = target.type == 'bigquery')}}
{% if var("subscriptions_warehouse_sources") %}
{% if 'stripe_subscriptions' in var("subscriptions_warehouse_sources") %}

{% if var("etl") == 'segment' %}
with source AS (
  {{ filter_segment_relation(relation=source('segment_stripe_subscriptions','subscriptions')) }}
),
renamed AS (
SELECT
  CONCAT('{{ var('stg_stripe_payments_id-prefix') }}',id) AS subscription_id,
  CONCAT('{{ var('stg_stripe_payments_id-prefix') }}',customer_id) AS customer_id,
  CONCAT('{{ var('stg_stripe_payments_id-prefix') }}',plan_id) AS plan_id,
  discount_id,
  quantity AS subscription_quantity,
  status AS subscription_status,
  tax_percent AS subscription_tax_percent,
  trial_start AS subscription_trial_start,
  trial_end AS subscription_trial_end,
  start AS subscription_start,
  ended_at AS subscription_ended_at,
  canceled_at AS subscription_canceled_at,
  cancel_at_period_end AS subscription_cancel_at_period_end,
  current_period_start AS subscription_current_period_start_date,
  current_period_end AS subscription_current_period_end_date,
  is_deleted AS subscription_is_deleted,
  metadata_organization_id AS organization_id,
  created AS subscription_created_ts,
   CAST(null AS {{ dbt_utils.type_timestamp() }}) AS subscription_last_modified_ts
FROM
  source
)
SELECT * FROM renamed
{% endif %}

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
