{% if not var("enable_custom_source_1") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
    select *
    from
    {{ source('custom_source_1','s_transactions' ) }}
),
renamed as (
  SELECT
      concat('custom_1-',id) as transaction_id,
      {{ cast() }}  as transaction_description,
      {{ cast() }}  as transaction_currency,
      cast(null as numeric) as transaction_exchange_rate,
      cast(null as numeric) as transaction_gross_amount,
      cast(null as numeric) as transaction_fee_amount,
      cast(null as numeric) as transaction_tax_amount,
      cast(null as numeric) as transaction_net_amount,
      {{ cast() }}  as transaction_status,
      {{ cast() }}  as transaction_type,
       {{ cast(datatype='timestamp') }} as transaction_created_ts,
       {{ cast(datatype='timestamp') }} as transaction_updated_ts
  FROM
    source
)
select * from renamed
