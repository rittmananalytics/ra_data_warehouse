{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_company_sources") %}
{% if 'xero_accounting' in var("crm_warehouse_company_sources") %}

{% if var("stg_xero_accounting_etl") == 'stitch' %}

WITH source as (
  {{ filter_stitch_relation(relation=var('stg_xero_accounting_stitch_contacts_table'),unique_column='contactid') }}
),
  phones as (SELECT companies.contactid, phones.phonetype, phones.phonenumber, phones.phoneareacode, phones.phonecountrycode
    FROM source companies,
 unnest(phones) as phones
 ),
      addresses as (SELECT companies.contactid, addresses.addresstype, addresses.addressline1, addresses.addressline2, addresses.addressline3, addresses.addressline4, addresses.city, addresses.region, addresses.country, addresses.postalcode
         FROM source companies,
 unnest(addresses) as addresses
),
renamed as (
 select
        concat('{{ var('stg_xero_accounting_id-prefix') }}',cast(contacts.contactid as string)) as company_id,
        replace(replace(replace(name,'Limited',''),'ltd',''),', Inc.','') as company_name,
        string_agg(distinct addresses.addressline1) as company_address,
        cast (null as {{ dbt_utils.type_string() }}) AS company_address2,
        string_agg(distinct addresses.city) as company_city,
        string_agg(distinct addresses.region) as company_state,
        string_agg(distinct addresses.country) as company_country,
        string_agg(distinct addresses.postalcode) as company_zip,
        replace(concat(replace(defaultphone.phonecountrycode,'+','00'),defaultphone.phoneareacode,defaultphone.phonenumber),' ','') as company_phone,
        cast (null as {{ dbt_utils.type_string() }}) AS company_website,
        cast (null as {{ dbt_utils.type_string() }}) AS company_industry,
        cast (null as {{ dbt_utils.type_string() }}) AS company_linkedin_company_page,
        cast (null as {{ dbt_utils.type_string() }}) AS company_linkedin_bio,
        cast (null as {{ dbt_utils.type_string() }}) AS company_twitterhandle,
        cast (null as {{ dbt_utils.type_string() }}) AS company_description,
        contacts.contactstatus as company_finance_status,
        cast (null as {{ dbt_utils.type_string() }})     as company_currency_code,
         cast(null as {{ dbt_utils.type_timestamp() }}) as company_created_date,
        contacts.updateddateutc as company_last_modified_date
 from source contacts left outer join addresses as addresses on contacts.contactid = addresses.contactid and addresses.addresstype = 'STREET'
 left outer join phones as mobilephone on contacts.contactid = mobilephone.contactid and mobilephone.phonetype = 'MOBILE'
 left outer join phones as defaultphone on contacts.contactid = defaultphone.contactid and mobilephone.phonetype = 'DEFAULT'
 where contacts.lastname is null
 group by 1,2,4,9,10,11,12,13,14,15,16,17,18,19)

{% elif var("stg_xero_accounting_etl") == 'fivetran' %}

with source as (
  select *
  from {{ var('stg_xero_accounting_fivetran_contacts_table') }}

),
      addresses as (SELECT contact_id, address_type, address_line_1, address_line_2, address_line_3, address_line_4, city, region, country, postal_code
         FROM {{ var('stg_xero_accounting_fivetran_contact_address_table') }}

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
