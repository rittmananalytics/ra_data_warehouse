{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("order_conversion_sources") %}
{% if 'custom' in var("order_conversion_sources") %}

with source as (

  select * from {{ source('custom_conversions','conversion_orders') }}

),
renamed as (
  SELECT
  ORDER_ID        as order_id,
  CUSTOMER_ID     as user_id,
  ORDER_TS        as order_ts,
  SESSION_ID      as session_id,
  CHECKOUT_ID     as checkout_id,
  TOTAL_REVENUE   as total_revenue,
  CURRENCY_CODE   as currency_code,
  UTM_SOURCE      as utm_source,
  UTM_MEDIUM      as utm_medium,
  UTM_CAMPAIGN    as utm_campaign,
  UTM_CONTENT     as utm_content,
  UTM_TERM        as utm_term,
  CHANNEL         as channel
from
  source
)
select
  *
from
  renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
