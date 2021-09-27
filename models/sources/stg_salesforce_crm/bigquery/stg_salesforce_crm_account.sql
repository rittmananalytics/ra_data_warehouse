{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_company_sources") %}
{% if 'salesforce_crm' in var("crm_warehouse_company_sources")  %}

with source as (

    select * from account

),

renamed as (

    select

        id as account_id,
        parentid as parent_id,
        ownerid as owner_id,
        type as account_type,
        billingstreet as company_street,
        billingcity as company_city,
        billingstate as company_state,
        billingcountry as company_country,
        billingpostalcode as company_zipcode,
        name as company_name,
        industry,
        description,
        numberofemployees as number_of_employees,
        lastactivitydate as last_activity_date,
        createddate as created_at,
        lastmodifieddate as updated_at

    from source

)

select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
