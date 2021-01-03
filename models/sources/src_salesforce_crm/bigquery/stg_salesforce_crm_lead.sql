{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contact_sources") %}
{% if 'salesforce_crm' in var("crm_warehouse_contact_sources")  %}

with source as (

    select * from lead

),

renamed as (

    select

        id as lead_id,
        ownerid as owner_id,
        convertedopportunityid as opportunity_id,
        convertedaccountid as account_id,
        convertedcontactid as contact_id,
        leadsource as lead_source,
        status,
        isconverted as is_converted,
        converteddate as converted_date,
        firstname as first_name,
        middlename as middle_name,
        lastname as last_name,
        name as full_name,
        title,
        email,
        phone as work_phone,
        mobilephone as mobile_phone,
        donotcall as can_call,
        isunreadbyowner as is_unread_by_owner,
        company as company_name,
        city as company_city,
        website,
        numberofemployees as number_of_employees,
        lastactivitydate as last_activity_date,
        createddate as created_at,
        lastmodifieddate as updated_at

    from source

)

select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
