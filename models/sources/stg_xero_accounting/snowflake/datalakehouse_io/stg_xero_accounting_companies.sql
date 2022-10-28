{{config(enabled = target.type == 'snowflake')}}
{% if var("crm_warehouse_company_sources") %}
{% if 'xero_accounting' in var("crm_warehouse_company_sources") %}

{% if var("stg_xero_accounting_etl") == 'datalakehouse_io' %}

with source as (
  select *
  from {{ source('datalakehouse_xero_accounting','contact') }}

),
      addresses as (SELECT contact_id, address_type, address_line_1, address_line_2, address_line_3, address_line_4, city, region, country, postal_code
         FROM {{ source('datalakehouse_xero_accounting','contact_address')  }}

),
renamed as (
 select
        concat('{{ var('stg_xero_accounting_id-prefix') }}',cast(contacts.contact_id as string)) as company_id,
        replace(replace(replace(name,'Limited',''),'ltd',''),', Inc.','') as company_name,
        {{ dbt.listagg("address_line_1","','") }} AS company_address,        
        address_line_2 AS company_address2,
        {{ dbt.listagg("city","','") }} AS company_city,
        {{ dbt.listagg("region","','") }} AS company_state,
        {{ dbt.listagg("country","','") }} AS company_country,
        {{ dbt.listagg("postal_code","','") }} AS company_zip,
        cast(null as {{ dbt_utils.type_string() }}) as company_phone,
        cast (null as {{ dbt_utils.type_string() }}) AS company_website,
        cast (null as {{ dbt_utils.type_string() }}) AS company_industry,
        cast (null as {{ dbt_utils.type_string() }}) AS company_linkedin_company_page,
        cast (null as {{ dbt_utils.type_string() }}) AS company_linkedin_bio,
        cast (null as {{ dbt_utils.type_string() }}) AS company_twitterhandle,
        cast (null as {{ dbt_utils.type_string() }}) AS company_description,
        contact_status as company_finance_status,
        cast (null as {{ dbt_utils.type_string() }})     as company_currency_code,
         cast(null as {{ dbt_utils.type_timestamp() }}) as company_created_date,
        cast(updated_date_utc as {{ dbt_utils.type_timestamp() }}) as company_last_modified_date
 from source as contacts
 left outer join addresses as addresses
 on contacts.contact_id = addresses.contact_id and addresses.address_type = 'STREET'
 where contacts.last_name is null
 group by 1,2,4,9,10,11,12,13,14,15,16,17,18,19)


{% endif %}
select * from renamed



{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
