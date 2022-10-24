#!/bin/bash
mkdir -p $1/macros
mkdir -p $1/models/tmp
dbt run-operation fivetran_utils.generate_columns_macro --args '{"table_name": "'$5'", "schema_name": "'$4'", "database_name":"'$3'"}' | tail -n +2 > $1/macros/get_$5_columns.sql
echo "select * from {{ var('$5') }}" > $1/models/tmp/$2__$5_tmp.sql
echo "" >> $1/models/$2__$5.sql
echo "with base as (

    select * 
    from {{ ref('$2__$5_tmp') }}

),

fields as (

    select
        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('$2__$5_tmp')),
                staging_columns=get_$5_columns()
            )
        }}
        
    from base
),

final as (
    
    select 
    -- rename here
    from fields
)

select * from final" >> $1/models/$2__$5.sql
