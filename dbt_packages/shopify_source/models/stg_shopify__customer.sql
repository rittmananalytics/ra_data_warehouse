with source as (

    select * from {{ ref('stg_shopify__customer_tmp') }}

),

renamed as (

    select
    
        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('stg_shopify__customer_tmp')),
                staging_columns=get_customer_columns()
            )
        }}

      --The below script allows for pass through columns.
      {% if var('customer_pass_through_columns') %}
      ,
      {{ var('customer_pass_through_columns') | join (", ")}}

      {% endif %}

      {{ fivetran_utils.source_relation() }}

    from source

)

select * from renamed

