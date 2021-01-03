{{config(enabled = target.type == 'bigquery')}}
{% if var("marketing_warehouse_deal_sources") %}
{% if 'salesforce_crm' in var("marketing_warehouse_deal_sources") %}

with source as (

    select * from opportunity

),

renamed as (

    select

        id as opportunity_id,
        accountid as account_id,
        ownerid as owner_id,
        name as company_name,
        description as opportunity_description,
        type as opportunity_type,
        leadsource as lead_source,
        fiscal as fiscal_yq,
        fiscalyear as fiscal_year,
        fiscalquarter as fiscal_quarter,
        probability,
        nextstep as next_step,
        hasopenactivity as has_open_activity,
        stagename as stage_name,
        forecastcategory as forecast_category,
        forecastcategoryname as forecast_category_name,
        iswon as is_won,
        isclosed as is_closed,
        closedate as closed_date,
        hasoverduetask as has_over_due_task,
        amount,
        lastactivitydate as last_activity_date,
        createddate as created_at,
        lastmodifieddate as updated_at
    from source

)

select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
