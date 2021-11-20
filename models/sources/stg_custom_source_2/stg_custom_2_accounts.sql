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
    {{ source('custom_source_2','s_accounts' ) }}
),
renamed as
(
SELECT  CONCAT('custom_2-',id) AS       account_id,
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
