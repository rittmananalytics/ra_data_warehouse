{% if not var("enable_looker_usage_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
with source as (
SELECT
  *
FROM
  {{ target.database}}.{{ var('fivetran_schema') }}.{{ var('fivetran_usage_table') }}
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  10,
  11,
  12,
  13)
renamed as (
SELECT
  created_time as usage_ts,
  pk as usage_id,
  client as account_name,
  title as usage_description,
  dialect as usage_type,
  explore as product_feature,
  name as user_name,
  rebuild_pdts_yes_no_ as is_cache_rebuild,
  status  as usage_outcome_status,
  source as usage_source_category,
  issuer_source as usage_source,
  1 as total_usages,
  round(average_runtime_in_seconds,2) as usage_response_time_mins
FROM
 source
)
