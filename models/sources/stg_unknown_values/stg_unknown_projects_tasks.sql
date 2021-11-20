{% if var("projects_warehouse_delivery_sources") %}

SELECT
       CONCAT('{{ var('stg_harvest_projects_id-prefix') }}',CAST(-999 AS string))                 AS task_id,
       'Unassigned'                          AS task_name,
       true AS task_billable_by_default,
       100 AS task_default_hourly_rate,
        CAST(null AS {{ dbt_utils.type_timestamp() }})          AS task_created_at,
        CAST(null AS {{ dbt_utils.type_timestamp() }})          AS task_updated_at,
       true           AS task_is_active

{% else %} {{config(enabled=false)}} {% endif %}
