{{config(enabled = target.type == 'bigquery')}}
{% if var("product_warehouse_usage_sources") %}
{% if 'bigquery_usage' in var("product_warehouse_usage_sources") %}


with source as (
    SELECT
      *
    FROM
      {{ source('bigquery_usage_product_usage', 'cloudaudit_data_access') }}
  ),
 renamed as
 (
  SELECT
        protopayload_auditlog.authenticationInfo.principalEmail                                                                       as product_account_id,
        coalesce(concat('{{ var('stg_bigquery_usage_id-prefix') }}',protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.queryPriority),'N/A')             as product_id,
        coalesce(concat('{{ var('stg_bigquery_usage_id-prefix') }}',protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.statementType),'N/A')            as product_sku_id,
        concat('{{ var('stg_bigquery_usage_id-prefix') }}',resource.labels.project_id)                                                                as company_id,
        resource.labels.project_id                                                                                                    as product_project_id,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.createTime                         as product_usage_billing_ts,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime                          as product_usage_start_ts,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime                            as product_usage_end_ts,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.location                                 as product_usage_location,
        cast (null as {{ dbt_utils.type_string() }})                     as product_usage_country,
        cast (null as {{ dbt_utils.type_string() }})                     as product_usage_region,
        cast (null as {{ dbt_utils.type_string() }})                     as product_usage_zone,
        "bytes"                                   as product_usage_unit,
        'GBP'                     as product_usage_currency,
        (protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes/1099511627776)*.72                    as product_usage_cost,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalProcessedBytes                       AS product_usage_amount,
        0.72                    as product_currency_conversion_rate,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.queryOutputRowCount as product_usage_row_count,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.query as product_usage_query_text,
        md5(lower(replace(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.query,' ',''))) as product_usage_query_hash,
        cast(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.queryPriority as string) as product_usage_priority,
        cast(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatus.state as string) as product_usage_status,
        cast(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatus.error.code as string) as product_usage_error_code,
        cast(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatus.error.message as string) as product_usage_error_status,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId as product_usage_job_id,
        cast(null as {{ dbt_utils.type_string() }}) as contact_id

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
