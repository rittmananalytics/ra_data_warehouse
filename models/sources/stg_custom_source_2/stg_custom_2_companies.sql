{% if not var("enable_custom_source_2") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source as (
    select *
    from
    {{ source('custom_source_2','s_customer' ) }}
),
renamed as (
SELECT
    concat('custom_2-',id)      as company_id,
    cast (null as {{ dbt_utils.type_string() }})     as company_name,
    cast (null as {{ dbt_utils.type_string() }})     as company_address,
    cast (null as {{ dbt_utils.type_string() }})     as company_address2,
    cast (null as {{ dbt_utils.type_string() }})     as company_city,
    cast (null as {{ dbt_utils.type_string() }})     as company_state,
    cast (null as {{ dbt_utils.type_string() }})     as company_country,
    cast (null as {{ dbt_utils.type_string() }})     as company_zip,
    cast (null as {{ dbt_utils.type_string() }})     as company_phone,
    cast (null as {{ dbt_utils.type_string() }})     as company_website,
    cast (null as {{ dbt_utils.type_string() }})     as company_industry,
    cast (null as {{ dbt_utils.type_string() }})     as company_linkedin_company_page,
    cast (null as {{ dbt_utils.type_string() }})     as company_linkedin_bio,
    cast (null as {{ dbt_utils.type_string() }})     as company_twitterhandle,
    cast (null as {{ dbt_utils.type_string() }})     as company_description,
    cast (null as {{ dbt_utils.type_string() }})     as company_finance_status,
    cast (null as {{ dbt_utils.type_string() }})     as company_currency_code,
     cast(null as {{ dbt_utils.type_timestamp() }})   as company_created_date,
     cast(null as {{ dbt_utils.type_timestamp() }})   as company_last_modified_date
FROM source
)
select * from renamed
