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
    {{ source('custom_source_2','s_customer' ) }}
),
renamed AS (
SELECT
    CONCAT('custom_2-',id)      AS company_id,
    CAST(null AS {{ dbt_utils.type_string() }})     AS company_name,
    CAST(null AS {{ dbt_utils.type_string() }})     AS company_address,
    CAST(null AS {{ dbt_utils.type_string() }})     AS company_address2,
    CAST(null AS {{ dbt_utils.type_string() }})     AS company_city,
    CAST(null AS {{ dbt_utils.type_string() }})     AS company_state,
    CAST(null AS {{ dbt_utils.type_string() }})     AS company_country,
    CAST(null AS {{ dbt_utils.type_string() }})     AS company_zip,
    CAST(null AS {{ dbt_utils.type_string() }})     AS company_phone,
    CAST(null AS {{ dbt_utils.type_string() }})     AS company_website,
    CAST(null AS {{ dbt_utils.type_string() }})     AS company_industry,
    CAST(null AS {{ dbt_utils.type_string() }})     AS company_linkedin_company_page,
    CAST(null AS {{ dbt_utils.type_string() }})     AS company_linkedin_bio,
    CAST(null AS {{ dbt_utils.type_string() }})     AS company_twitterhandle,
    CAST(null AS {{ dbt_utils.type_string() }})     AS company_description,
    CAST(null AS {{ dbt_utils.type_string() }})     AS company_finance_status,
    CAST(null AS {{ dbt_utils.type_string() }})     AS company_currency_code,
     CAST(null AS {{ dbt_utils.type_timestamp() }})   AS company_created_date,
     CAST(null AS {{ dbt_utils.type_timestamp() }})   AS company_last_modified_date
FROM source
)
SELECT * FROM renamed
