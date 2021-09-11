{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_company_sources") %}
{% if 'xero_accounting' in var("crm_warehouse_company_sources") %}

{% if var("stg_xero_accounting_etl") == 'fivetran' %}

with source as (
  select *
  from {{ source('fivetran_xero_accounting','contact') }}

),
      addresses as (SELECT contact_id, address_type, address_line_1, address_line_2, address_line_3, address_line_4, city, region, country, postal_code
         FROM {{ source('fivetran_xero_accounting','contact_address')  }}

),
renamed as (
 select
        concat('{{ var('stg_xero_accounting_id-prefix') }}',cast(contacts.contact_id as string)) as company_id,
        replace(replace(replace(name,'Limited',''),'ltd',''),', Inc.','') as company_name,
        string_agg(distinct address_line_1) as company_address,
        address_line_2 AS company_address2,
        string_agg(distinct city) as company_city,
        string_agg(distinct region) as company_state,
        string_agg(distinct country) as company_country,
        string_agg(distinct postal_code) as company_zip,
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
        timestamp(updated_date_utc) as company_last_modified_date
 from source as contacts
 left outer join addresses as addresses
 on contacts.contact_id = addresses.contact_id and addresses.address_type = 'STREET'
 where contacts.last_name is null
 group by 1,2,4,9,10,11,12,13,14,15,16,17,18,19)


{% endif %}
select * from renamed



{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
