{% if not var("enable_custom_source_2") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
    SELECT *
    from
    {{ source('custom_source_2','s_transactions' ) }}
),
renamed AS (
  SELECT
      CONCAT('custom_2-',id) AS transaction_id,
      CAST(null AS {{ dbt_utils.type_string() }})  AS transaction_description,
      CAST(null AS {{ dbt_utils.type_string() }})  AS transaction_currency,
      CAST(null AS numeric) AS transaction_exchange_rate,
      CAST(null AS numeric) AS transaction_gross_amount,
      CAST(null AS numeric) AS transaction_fee_amount,
      CAST(null AS numeric) AS transaction_tax_amount,
      CAST(null AS numeric) AS transaction_net_amount,
      CAST(null AS {{ dbt_utils.type_string() }})  AS transaction_status,
      CAST(null AS {{ dbt_utils.type_string() }})  AS transaction_type,
       CAST(null AS {{ dbt_utils.type_timestamp() }}) AS transaction_created_ts,
       CAST(null AS {{ dbt_utils.type_timestamp() }}) AS transaction_updated_ts
  FROM
    source
)
SELECT * FROM renamed
