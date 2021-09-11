{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_company_sources") %}
{% if 'hubspot_crm' in var("crm_warehouse_company_sources") %}

WITH source AS (
    {{ filter_stitch_relation(relation=source('stitch_stripe_payments','charges'),unique_column='id') }}
),
renamed as (
select * from (
SELECT
concat('{{ var('stg_stripe_payments_id-prefix') }}',replace(replace(replace(metadata.client_name,'Limited',''),'ltd',''),', Inc.','')) AS company_id,
    replace(replace(replace(metadata.client_name,'Limited',''),'ltd',''),', Inc.','') AS company_name,
    cast (null as {{ dbt_utils.type_string() }}) as company_address,
    cast (null as {{ dbt_utils.type_string() }}) AS company_address2,
    cast (null as {{ dbt_utils.type_string() }}) AS company_city,
    cast (null as {{ dbt_utils.type_string() }}) AS company_state,
    cast (null as {{ dbt_utils.type_string() }}) AS company_country,
    cast (null as {{ dbt_utils.type_string() }}) AS company_zip,
    cast (null as {{ dbt_utils.type_string() }}) AS company_phone,
    cast (null as {{ dbt_utils.type_string() }}) AS company_website,
    cast (null as {{ dbt_utils.type_string() }}) AS company_industry,
    cast (null as {{ dbt_utils.type_string() }}) AS company_linkedin_company_page,
    cast (null as {{ dbt_utils.type_string() }}) AS company_linkedin_bio,
    cast (null as {{ dbt_utils.type_string() }}) AS company_twitterhandle,
    cast (null as {{ dbt_utils.type_string() }}) AS company_description,
    cast (null as {{ dbt_utils.type_string() }}) as company_finance_status,
    cast (null as {{ dbt_utils.type_string() }})     as company_currency_code,
    min(created) over (partition by metadata.client_name) as company_created_date,
    max(created) over (partition by metadata.client_name) as company_last_modified_date
    FROM source )
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19)
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
