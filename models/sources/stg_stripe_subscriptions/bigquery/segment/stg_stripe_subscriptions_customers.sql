{{config(enabled = target.type == 'bigquery')}}
{% if var("subscriptions_warehouse_sources") %}
{% if 'stripe_subscriptions' in var("subscriptions_warehouse_sources") %}

with source AS (
  {{ filter_segment_relation(relation=source('segment_stripe_subscriptions','customers')) }}

),
renamed AS (
  SELECT CONCAT('stg_stripe_payments_segment-',metadata_CLIENTREPLACEME_user_id) AS customer_id,
  email AS customer_email,
  description AS customer_description,
  CONCAT('{{ var('stg_stripe_payments_id-prefix') }}',id) AS customer_alternative_id,
  CAST(null AS {{ dbt_utils.type_string() }}) AS customer_plan,
  metadata_source AS customer_source,
  CAST(null AS {{ dbt_utils.type_string() }}) AS customer_type,
  CAST(null AS {{ dbt_utils.type_string() }}) AS customer_industry,
  CAST(null AS {{ dbt_utils.type_string() }}) AS customer_currency,
  CAST(null AS {{ dbt_utils.type_boolean() }}) AS customer_is_enterprise,
  CAST(null AS {{ dbt_utils.type_boolean() }}) AS customer_is_delinquent,
  CAST(null AS {{ dbt_utils.type_boolean() }}) AS customer_is_deleted,
   CAST(null AS {{ dbt_utils.type_timestamp() }}) AS customer_created_date,
   CAST(null AS {{ dbt_utils.type_timestamp() }}) AS customer_last_modified_date
FROM source
)
SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
