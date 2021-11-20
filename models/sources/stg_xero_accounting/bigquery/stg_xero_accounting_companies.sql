{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_company_sources") %}
{% if 'xero_accounting' in var("crm_warehouse_company_sources") %}

{% if var("stg_xero_accounting_etl") == 'fivetran' %}

with source AS (
  SELECT *
  FROM {{ source('fivetran_xero_accounting','contact') }}

),
      addresses AS (SELECT contact_id, address_type, address_line_1, address_line_2, address_line_3, address_line_4, city, region, country, postal_code
         FROM {{ source('fivetran_xero_accounting','contact_address')  }}

),
renamed AS (
 SELECT
        CONCAT('{{ var('stg_xero_accounting_id-prefix') }}',CAST(contacts.contact_id AS string)) AS company_id,
        replace(replace(replace(name,'Limited',''),'ltd',''),', Inc.','') AS company_name,
        string_agg(distinct address_line_1) AS company_address,
        address_line_2 AS company_address2,
        string_agg(distinct city) AS company_city,
        string_agg(distinct region) AS company_state,
        string_agg(distinct country) AS company_country,
        string_agg(distinct postal_code) AS company_zip,
        CAST(null AS {{ dbt_utils.type_string() }}) AS company_phone,
        CAST(null AS {{ dbt_utils.type_string() }}) AS company_website,
        CAST(null AS {{ dbt_utils.type_string() }}) AS company_industry,
        CAST(null AS {{ dbt_utils.type_string() }}) AS company_linkedin_company_page,
        CAST(null AS {{ dbt_utils.type_string() }}) AS company_linkedin_bio,
        CAST(null AS {{ dbt_utils.type_string() }}) AS company_twitterhandle,
        CAST(null AS {{ dbt_utils.type_string() }}) AS company_description,
        contact_status AS company_finance_status,
        CAST(null AS {{ dbt_utils.type_string() }})     AS company_currency_code,
         CAST(null AS {{ dbt_utils.type_timestamp() }}) AS company_created_date,
        timestamp(updated_date_utc) AS company_last_modified_date
 FROM source AS contacts
 left outer join addresses AS addresses
 on contacts.contact_id = addresses.contact_id and addresses.address_type = 'STREET'
 where contacts.last_name is null
 group by 1,2,4,9,10,11,12,13,14,15,16,17,18,19)


{% endif %}
SELECT * FROM renamed



{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
