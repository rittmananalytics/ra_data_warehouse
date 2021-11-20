{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_company_sources") %}
{% if 'hubspot_crm' in var("crm_warehouse_company_sources") %}

WITH source AS (
    {{ filter_stitch_relation(relation=source('stitch_stripe_payments','charges'),unique_column='id') }}
),
renamed AS (
SELECT * FROM (
SELECT
CONCAT('{{ var('stg_stripe_payments_id-prefix') }}',replace(replace(replace(metadata.client_name,'Limited',''),'ltd',''),', Inc.','')) AS company_id,
    replace(replace(replace(metadata.client_name,'Limited',''),'ltd',''),', Inc.','') AS company_name,
    CAST(null AS {{ dbt_utils.type_string() }}) AS company_address,
    CAST(null AS {{ dbt_utils.type_string() }}) AS company_address2,
    CAST(null AS {{ dbt_utils.type_string() }}) AS company_city,
    CAST(null AS {{ dbt_utils.type_string() }}) AS company_state,
    CAST(null AS {{ dbt_utils.type_string() }}) AS company_country,
    CAST(null AS {{ dbt_utils.type_string() }}) AS company_zip,
    CAST(null AS {{ dbt_utils.type_string() }}) AS company_phone,
    CAST(null AS {{ dbt_utils.type_string() }}) AS company_website,
    CAST(null AS {{ dbt_utils.type_string() }}) AS company_industry,
    CAST(null AS {{ dbt_utils.type_string() }}) AS company_linkedin_company_page,
    CAST(null AS {{ dbt_utils.type_string() }}) AS company_linkedin_bio,
    CAST(null AS {{ dbt_utils.type_string() }}) AS company_twitterhandle,
    CAST(null AS {{ dbt_utils.type_string() }}) AS company_description,
    CAST(null AS {{ dbt_utils.type_string() }}) AS company_finance_status,
    CAST(null AS {{ dbt_utils.type_string() }})     AS company_currency_code,
    min(created) over (PARTITION BYmetadata.client_name) AS company_created_date,
    max(created) over (PARTITION BYmetadata.client_name) AS company_last_modified_date
    FROM source )
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19)
SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
