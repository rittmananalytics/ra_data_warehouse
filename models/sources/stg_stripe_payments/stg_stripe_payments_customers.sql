{% if not var("enable_stripe_payments_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
{% if var("etl") == 'segment' %}
with source as (
  {{ filter_segment_table(var('segment_schema'),var('segment_customers_table')) }}
),
renamed as (
  SELECT
    concat('{{ var('id-prefix') }}',id) as customer_id,
    email as customer_email,
    description as customer_description,    
    metadata_switcher_user_id as customer_user_id,
    account_balance as customer_account_balance,
    currency as customer_currency,
    metadata_source as customer__source,
    delinquent as customer_is_delinquent,
    is_deleted as customer_is_deleted,
    cast(null as timestamp) as customer_created_ts,
    received_at as customer_last_modified_ts
from source
)
select * from renamed
{% endif %}
