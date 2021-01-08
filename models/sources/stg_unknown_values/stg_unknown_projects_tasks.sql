{% if var("projects_warehouse_delivery_sources") %}

select
       concat('{{ var('stg_harvest_projects_id-prefix') }}',cast(-999 as string))                 as task_id,
       'Unassigned'                          as task_name,
       true as task_billable_by_default,
       100 as task_default_hourly_rate,
       cast(null as timestamp)          as task_created_at,
       cast(null as timestamp)          as task_updated_at,
       true           as task_is_active

{% else %} {{config(enabled=false)}} {% endif %}
