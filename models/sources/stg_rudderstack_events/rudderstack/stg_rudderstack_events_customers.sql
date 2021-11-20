{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("product_warehouse_event_sources") %}
{% if 'rudderstack_events_page' in var("product_warehouse_event_sources") %}

with source AS (

    SELECT * FROM {{ source('rudderstack', 'users') }}

),
renamed AS (
   SELECT CONCAT('stg_rudderstack_events_id-prefix',id) AS customer_id,
   email AS customer_email,
  CAST(null AS {{ dbt_utils.type_string() }}) AS customer_description,
  CAST(null AS {{ dbt_utils.type_string() }}) AS customer_source,
  CAST(null AS {{ dbt_utils.type_string() }}) AS customer_type,
  CAST(null AS {{ dbt_utils.type_string() }}) AS customer_industry,
  CAST(null AS {{ dbt_utils.type_string() }}) AS customer_currency,
  CAST(null AS {{ dbt_utils.type_string() }}) AS customer_is_enterprise,
  CAST(null AS {{ dbt_utils.type_boolean() }}) AS customer_is_delinquent,
  CAST(null AS {{ dbt_utils.type_boolean() }}) AS customer_is_deleted,
  min(received_at) over (PARTITION BYid) AS customer_created_date,
  max(received_at) over (PARTITION BYid) AS customer_last_modified_date
FROM
 source
)
SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
