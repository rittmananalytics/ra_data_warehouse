{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_company_sources") %}
{% if 'salesforce_crm' in var("crm_warehouse_company_sources")  %}

with source AS (

    SELECT * FROM account

),

renamed AS (

    SELECT

        id AS account_id,
        parentid AS parent_id,
        ownerid AS owner_id,
        type AS account_type,
        billingstreet AS company_street,
        billingcity AS company_city,
        billingstate AS company_state,
        billingcountry AS company_country,
        billingpostalcode AS company_zipcode,
        name AS company_name,
        industry,
        description,
        numberofemployees AS number_of_employees,
        lastactivitydate AS last_activity_date,
        createddate AS created_at,
        lastmodifieddate AS updated_at

    FROM source

)

SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
