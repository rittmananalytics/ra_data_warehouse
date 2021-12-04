{% if var("marketing_warehouse_ad_sources") %}

with unioned as (

    {{ dbt_utils.union_relations(get_ad_network_staging_files()) }}

)

select *
from unioned

{% else %} {{config(enabled=false)}} {% endif %}
