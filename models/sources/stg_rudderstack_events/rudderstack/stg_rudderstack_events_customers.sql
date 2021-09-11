{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("product_warehouse_event_sources") %}
{% if 'rudderstack_events_page' in var("product_warehouse_event_sources") %}

with source as (

    select * from {{ source('rudderstack', 'users') }}

),
renamed as (
   select concat('stg_rudderstack_events_id-prefix',id) as customer_id,
   email as customer_email,
  cast(null as {{ dbt_utils.type_string() }}) as customer_description,
  cast(null as {{ dbt_utils.type_string() }}) as customer_source,
  cast(null as {{ dbt_utils.type_string() }}) as customer_type,
  cast(null as {{ dbt_utils.type_string() }}) as customer_industry,
  cast(null as {{ dbt_utils.type_string() }}) as customer_currency,
  cast(null as {{ dbt_utils.type_string() }}) as customer_is_enterprise,
  cast(null as {{ dbt_utils.type_boolean() }}) as customer_is_delinquent,
  cast(null as {{ dbt_utils.type_boolean() }}) as customer_is_deleted,
  min(received_at) over (partition by id) as customer_created_date,
  max(received_at) over (partition by id) as customer_last_modified_date
FROM
 source
)
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
