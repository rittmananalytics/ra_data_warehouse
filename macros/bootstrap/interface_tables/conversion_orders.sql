{%- macro conversion_orders() -%}

CREATE TABLE IF NOT EXISTS {{ target.database }}.{{ target.schema }}_staging.conversion_orders (
  ORDER_ID               STRING,
  CUSTOMER_ID            STRING,
  ORDER_TS               TIMESTAMP,
  SESSION_ID             STRING,
  CHECKOUT_ID            STRING,
  TOTAL_REVENUE          FLOAT64,
  CURRENCY_CODE          STRING,
  UTM_SOURCE             STRING,
  UTM_MEDIUM             STRING,
  UTM_CAMPAIGN           STRING,
  UTM_CONTENT            STRING,
  UTM_TERM               STRING,
  CHANNEL                STRING,
  LAST_UPDATED_AT_TS     TIMESTAMP
)
;
{% endmacro %}
