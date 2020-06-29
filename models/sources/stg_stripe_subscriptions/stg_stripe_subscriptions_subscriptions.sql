{% if not var("enable_stripe_subscriptions_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
{% if var("etl") == 'segment' %}
with source as (
  {{ filter_segment_table(var('segment_schema'),var('segment_subscriptions_table')) }}
),
renamed as (
SELECT
  concat('{{ var('id-prefix') }}',id) as subscription_id,
  concat('{{ var('id-prefix') }}',customer_id) as customer_id,
  concat('{{ var('id-prefix') }}',plan_id) as plan_id,
  discount_id,
  quantity as subscription_quantity,
  status as subscription_status,
  tax_percent as subscription_tax_percent,
  trial_start as subscription_trial_start,
  trial_end as subscription_trial_end,
  start as subscription_start,
  ended_at as subscription_ended_at,
  canceled_at as subscription_canceled_at,
  cancel_at_period_end as subscription_cancel_at_period_end,
  current_period_start as subscription_current_period_start_date,
  current_period_end as subscription_current_period_end_date,
  is_deleted as subscription_is_deleted,
  metadata_organization_id as organization_id,
  created as subscription_created_ts,
  cast(null as timestamp) as subscription_last_modified_ts
FROM
  source
)
select * from renamed
{% endif %}
