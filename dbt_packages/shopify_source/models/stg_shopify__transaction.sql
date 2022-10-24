with source as (

    select * from {{ ref('stg_shopify__transaction_tmp') }}

),

renamed as (

    select

        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('stg_shopify__transaction_tmp')),
                staging_columns=get_transaction_columns()
            )
        }}

         --The below script allows for pass through columns.
        {% if var('transaction_pass_through_columns') %}
        ,
        {{ var('transaction_pass_through_columns') | join (", ")}}

        {% endif %}

      {{ fivetran_utils.source_relation() }}

    from source
    where not test

)

select * from renamed
