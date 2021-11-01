{{ config(enabled=var('api_source') == 'adwords') }}

with source as (

    select *
    from {{ ref('stg_google_ads__criteria_performance_tmp') }}

),

renamed as (

    select
    
        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('stg_google_ads__criteria_performance_tmp')),
                staging_columns=get_criteria_performance_columns()
            )
        }}

        {% for metric in var('google_ads__criteria_passthrough_metrics') %}
        , {{ metric }}
        {% endfor %}

    from source

)

select * from renamed