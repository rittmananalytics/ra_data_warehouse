{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("product_warehouse_event_sources") %}
{% if 'segment_events_page' in var("product_warehouse_event_sources") %}

with source AS (

    SELECT * FROM {{ source('segment', 'users') }}

),
renamed AS (
  SELECT CONCAT('stg_segment_events_id-prefix',id) AS user_id,
  email AS customer_email,
  min(received_at) over (PARTITION BYid) AS user_created_date,
  max(received_at) over (PARTITION BYid) AS user_last_modified_date
FROM
 source
)
SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
