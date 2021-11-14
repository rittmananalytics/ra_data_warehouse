{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("product_warehouse_event_sources") %}
{% if 'segment_events_page' in var("product_warehouse_event_sources") %}

with source as (

    select * from {{ source('segment', 'users') }}

),
renamed as (
  select concat('stg_segment_events_id-prefix',id) as user_id,
  email as customer_email,
  min(received_at) over (partition by id) as user_created_date,
  max(received_at) over (partition by id) as user_last_modified_date
FROM
 source
)
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
