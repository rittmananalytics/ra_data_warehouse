--To disable this model, set the shopify__using_order_adjustment variable within your dbt_project.yml file to False.
{{ config(enabled=var('shopify__using_order_adjustment', True)) }}

with source as (

    select * 
    from {{ ref('stg_shopify__order_adjustment_tmp') }}

),

renamed as (

    select
        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('stg_shopify__order_adjustment_tmp')),
                staging_columns=get_order_adjustment_columns()
            )
        }}

      {{ fivetran_utils.source_relation() }}
        
    from source
)

select * from renamed
