{% if not var("enable_stripe_payments_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
with source as (
  {{ filter_segment_table(var('segment_schema'),var('segment_customers_table')) }}
),
renamed as (
  select concat('segment-',metadata_CLIENTREPLACEME_user_id) as customer_id,
  email as customer_email,
  description as customer_description,
  concat('{{ var('id-prefix') }}',id) as customer_alternative_id,
  cast(null as string) as customer_plan,
  metadata_source as customer_source,
  cast(null as string) as customer_type,
  cast(null as string) as customer_industry,
  cast(null as string) as customer_currency,
  cast(null as boolean) as customer_is_enterprise,
  cast(null as boolean) as customer_is_delinquent,
  cast(null as boolean) as customer_is_deleted,
  cast(null as timestamp) as customer_created_date,
  cast(null as timestamp) as customer_last_modified_date
from source
)
select * from renamed
