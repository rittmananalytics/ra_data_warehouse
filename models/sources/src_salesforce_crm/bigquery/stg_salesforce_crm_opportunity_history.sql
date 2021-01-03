{{config(enabled = target.type == 'bigquery')}}
{% if var("marketing_warehouse_deal_sources") %}
{% if 'salesforce_crm' in var("marketing_warehouse_deal_sources") %}

with source as (

    select * from opportunityhistory

),

renamed as (

    select

        id as opportunity_history_id,
        opportunityid as opportunity_id,
        createdbyid as created_by_id,
        probability,
        stagename as stage_name,
        forecastcategory as forecast_category,
        amount,
        expectedrevenue as expected_revenue,
        createddate as created_date

    from source

)

select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
