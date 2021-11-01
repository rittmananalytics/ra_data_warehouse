{{ config(enabled=var('google_ads_api_source') == 'adwords') }}

with source as (

  select *
  from {{ source('adwords','criteria_performance') }}

),

renamed as (

    select

        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('stg_google_ads__criteria_performance_tmp')),
                staging_columns=get_google_ads_criteria_performance_columns()
            )
        }}

        {% for metric in [] %}
        , {{ metric }}
        {% endfor %}

    from source

)

select * from renamed
