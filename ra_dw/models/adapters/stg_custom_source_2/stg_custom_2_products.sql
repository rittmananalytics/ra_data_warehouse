{% if not var("enable_custom_source_2") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
    select *
    from
    {{ source('custom_source_2','s_transactions' ) }}
),
renamed as (
  SELECT
      concat('custom_2-',id) as transaction_id,
      cast(null as string)  as transaction_description,
      cast(null as string)  as transaction_currency,
      cast(null as numeric) as transaction_exchange_rate,
      cast(null as numeric) as transaction_gross_amount,
      cast(null as numeric) as transaction_fee_amount,
      cast(null as numeric) as transaction_tax_amount,
      cast(null as numeric) as transaction_net_amount,
      cast(null as string)  as transaction_status,
      cast(null as string)  as transaction_type,
      cast(null as timestamp) as transaction_created_ts,
      cast(null as timestamp) as transaction_updated_ts
  FROM
    source
)
select * from renamed
