{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_company_sources") %}
{% if 'hubspot_crm' in var("crm_warehouse_company_sources") %}

WITH source AS (
    {{ filter_stitch_relation(relation=var('stg_stripe_payments_stitch_charges_table'),unique_column='id') }}
),
renamed as (
select * from (
SELECT
concat('{{ var('stg_stripe_payments_id-prefix') }}',replace(replace(replace(metadata.client_name,'Limited',''),'ltd',''),', Inc.','')) AS company_id,
    replace(replace(replace(metadata.client_name,'Limited',''),'ltd',''),', Inc.','') AS company_name,
    cast (null as string) as company_address,
    cast (null as string) AS company_address2,
    cast (null as string) AS company_city,
    cast (null as string) AS company_state,
    cast (null as string) AS company_country,
    cast (null as string) AS company_zip,
    cast (null as string) AS company_phone,
    cast (null as string) AS company_website,
    cast (null as string) AS company_industry,
    cast (null as string) AS company_linkedin_company_page,
    cast (null as string) AS company_linkedin_bio,
    cast (null as string) AS company_twitterhandle,
    cast (null as string) AS company_description,
    cast (null as string) as company_finance_status,
    cast (null as string)     as company_currency_code,
    min(created) over (partition by metadata.client_name) as company_created_date,
    max(created) over (partition by metadata.client_name) as company_last_modified_date
    FROM source )
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19)
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
