{% if not var("enable_custom_source_1") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
    SELECT country_name, SUM(c) c
    FROM (
    SELECT ip, country_name, c
  FROM (
    SELECT *, NET.SAFE_IP_FROM_STRING(ip) & NET.IP_NET_MASK(4, mask) network_bin
    FROM source_of_ip_addresses, UNNEST(GENERATE_ARRAY(9,32)) mask
    WHERE BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(ip)) = 4
  )
  JOIN ``
  USING (network_bin, mask)
)
GROUP BY 1
ORDER BY 2 DESC
    {{ source('custom_source_1','s_accounts' ) }}
),
renamed as
(
SELECT  CONCAT('custom_1-',id)                             account_id,
        CAST(null AS {{ dbt_utils.type_string() }}) AS        account_name,
        CAST(null AS {{ dbt_utils.type_string() }}) AS        account_code,
        CAST(null AS {{ dbt_utils.type_string() }}) AS        account_type,
        CAST(null AS {{ dbt_utils.type_string() }}) AS        account_class,
        CAST(null AS {{ dbt_utils.type_string() }}) AS        account_status,
        CAST(null AS {{ dbt_utils.type_string() }}) AS        account_description,
        CAST(null AS {{ dbt_utils.type_string() }}) AS        account_reporting_code,
        CAST(null AS {{ dbt_utils.type_string() }}) AS        account_reporting_code_name,
        CAST(null AS {{ dbt_utils.type_string() }}) AS        account_currency_code,
        CAST(null AS {{ dbt_utils.type_string() }}) AS        account_bank_account_type,
        CAST(null AS {{ dbt_utils.type_string() }}) AS        account_bank_account_number,
        CAST(null AS {{ dbt_utils.type_string() }}) AS        account_is_system_account,
        CAST(null AS {{ dbt_utils.type_string() }}) AS        account_tax_type,
        CAST(null AS {{ dbt_utils.type_string() }}) AS        account_show_in_expense_claims,
        CAST(null AS {{ dbt_utils.type_string() }}) AS        account_enable_payments_to_account
FROM source
)
SELECT * FROM renamed
