{% if var("marketing_warehouse_ad_sources") %}

with unioned AS (

    {{ dbt_utils.union_relations(get_ad_network_staging_files()) }}

)

SELECT *
FROM unioned

{% else %} {{config(enabled=false)}} {% endif %}
