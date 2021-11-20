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
        protopayload_auditlog.authenticationInfo.principalEmail                                                                       AS product_account_id,
        coalesce(CONCAT('{{ var('stg_bigquery_usage_id-prefix') }}',protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.queryPriority),'N/A')             AS product_id,
        coalesce(CONCAT('{{ var('stg_bigquery_usage_id-prefix') }}',protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.statementType),'N/A')            AS product_sku_id,
        CONCAT('{{ var('stg_bigquery_usage_id-prefix') }}',resource.labels.project_id)                                                                AS company_id,
        resource.labels.project_id                                                                                                    AS product_project_id,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.createTime                         AS product_usage_billing_ts,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime                          AS product_usage_start_ts,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime                            AS product_usage_end_ts,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.location                                 AS product_usage_location,
        CAST(null AS {{ dbt_utils.type_string() }})                     AS product_usage_country,
        CAST(null AS {{ dbt_utils.type_string() }})                     AS product_usage_region,
        CAST(null AS {{ dbt_utils.type_string() }})                     AS product_usage_zone,
        "bytes"                                   AS product_usage_unit,
        'GBP'                     AS product_usage_currency,
        (protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes/1099511627776)*.72                    AS product_usage_cost,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalProcessedBytes                       AS product_usage_amount,
        0.72                    AS product_currency_conversion_rate,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.queryOutputRowCount AS product_usage_row_count,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.query AS product_usage_query_text,
        md5(lower(replace(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.query,' ',''))) AS product_usage_query_hash,
        CAST(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.queryPriority AS string) AS product_usage_priority,
        CAST(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatus.state AS string) AS product_usage_status,
        CAST(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatus.error.code AS string) AS product_usage_error_code,
        CAST(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatus.error.message AS string) AS product_usage_error_status,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId AS product_usage_job_id,
        CAST(null AS {{ dbt_utils.type_string() }}) AS contact_id

        FROM
          source
        where
          protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.statementType is not null
        )
SELECT
 *
FROM
 renamed

 {% else %} {{config(enabled=false)}} {% endif %}
 {% else %} {{config(enabled=false)}} {% endif %}
