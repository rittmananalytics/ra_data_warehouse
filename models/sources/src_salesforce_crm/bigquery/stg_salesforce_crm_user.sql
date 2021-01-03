{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contact_sources") %}
{% if 'salesforce_crm' in var("crm_warehouse_contact_sources")  %}

with source as (

    select * from user

),

renamed as (

    select

        id as user_id,
        accountid as account_id,
        firstname as first_name,
        middlename as middle_name,
        lastname as last_name,
        name as full_name,
        username as user_name,
        title,
        email,
        phone as work_phone,
        mobilephone as mobile_phone,
        createddate as created_at,
        lastmodifieddate as updated_at

    from source

)

select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
