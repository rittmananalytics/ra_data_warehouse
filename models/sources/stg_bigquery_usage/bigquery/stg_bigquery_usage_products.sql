{{config(enabled = target.type == 'bigquery')}}
{% if var("product_warehouse_usage_sources") %}
{% if 'bigquery_usage' in var("product_warehouse_usage_sources") %}


with source AS (
    SELECT
      *
    FROM
      {{ source('bigquery_usage_product_usage', 'cloudaudit_data_access') }}
  ),
 renamed as
 (
  SELECT
    coalesce(CONCAT('{{ var('stg_bigquery_usage_id-prefix') }}',protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.statementType),'N/A')                     AS product_sku_id,
    coalesce(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.statementType,'N/A')                      AS product_sku_name,
    coalesce(CONCAT('{{ var('stg_bigquery_usage_id-prefix') }}',protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.queryPriority),'N/A')                    AS product_id,
    coalesce(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.queryPriority,'N/A')                      AS product_name,
    CONCAT('{{ var('stg_bigquery_usage_id-prefix') }}','BigQuery Usage Export')                       AS product_source_id,
    'BigQuery Usage Export'                        AS product_source_name
FROM source
    group by 1,2,3,4,5,6)
SELECT
    *
FROM
  renamed

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
