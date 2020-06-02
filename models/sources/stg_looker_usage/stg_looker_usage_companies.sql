{% if not var("enable_looker_usage_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source as (
    select client
    from
    {{ target.database}}.{{ var('fivetran_schema') }}.{{ var('fivetran_usage_table') }}
),
renamed as (
SELECT
    concat('{{ var('id-prefix') }}',replace(client,' ','_')) AS company_id,
    client                    as company_name,
    cast (null as string)     as company_address,
    cast (null as string)     as company_address2,
    cast (null as string)     as company_city,
    cast (null as string)     as company_state,
    cast (null as string)     as company_country,
    cast (null as string)     as company_zip,
    cast (null as string)     as company_phone,
    cast (null as string)     as company_website,
    cast (null as string)     as company_industry,
    cast (null as string)     as company_linkedin_company_page,
    cast (null as string)     as company_linkedin_bio,
    cast (null as string)     as company_twitterhandle,
    cast (null as string)     as company_description,
    cast (null as string)     as company_finance_status,
    min(created) over (partition by metadata.client_name) as company_created_date,
    max(created) over (partition by metadata.client_name) as company_last_modified_date
FROM source
)
select * from renamed
