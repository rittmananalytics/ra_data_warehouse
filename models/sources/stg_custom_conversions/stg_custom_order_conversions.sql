{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("order_conversion_sources") %}
{% if 'custom' in var("order_conversion_sources") %}

with source AS (

  SELECT * FROM {{ source('custom_conversions','conversion_orders') }}

),
renamed AS (
  SELECT
  ORDER_ID        AS order_id,
  CUSTOMER_ID     AS user_id,
  ORDER_TS        AS order_ts,
  SESSION_ID      AS session_id,
  CHECKOUT_ID     AS checkout_id,
  TOTAL_REVENUE   AS total_revenue,
  CURRENCY_CODE   AS currency_code,
  UTM_SOURCE      AS utm_source,
  UTM_MEDIUM      AS utm_medium,
  UTM_CAMPAIGN    AS utm_campaign,
  UTM_CONTENT     AS utm_content,
  UTM_TERM        AS utm_term,
  CHANNEL         AS channel
from
  source
)
SELECT
  *
from
  renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
