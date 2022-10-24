with source as (

    select *
    from {{ ref('fill_staging_columns_input') }}

),

renamed as (

    select
    
        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('fill_staging_columns_input')),
                staging_columns=get_fill_staging_columns_columns()
            )
        }}

    from source

)

select * 
from renamed