{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contact_sources") %}
{% if 'salesforce_crm' in var("crm_warehouse_contact_sources")  %}

with source as (

    select * from contact

),

renamed as (

    select
        id as contact_id,
        ownerid as owner_id,
        accountid as account_id,
        firstname as first_name,
        middlename as middle_name,
        lastname as last_name,
        name as full_name,
        title,
        email,
        phone as work_phone,
        mobilephone as mobile_phone,
        lastactivitydate as last_activity_date,
        createddate as created_at,
        lastmodifieddate as updated_at
    from source

)

select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
