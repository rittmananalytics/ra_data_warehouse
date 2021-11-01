{{ config(materialized='table') }}

with unioned as (

    {{ dbt_utils.union_relations(get_staging_files()) }}

)

select *
from unioned