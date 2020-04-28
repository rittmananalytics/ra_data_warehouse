{% if not var("enable_looker_usage_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
  select * from
  (
    select *,
           max(_fivetran_synced) OVER (PARTITION BY pk ORDER BY _fivetran_synced RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_fivetran_synced
    from
    {{ source(
      'looker_usage',
      's_usage_stats'
    ) }}
  )
  where _fivetran_synced = max_fivetran_synced
),
renamed as (
SELECT
      pk as usage_id,
      concat('looker-',client) AS   customer_id,
      created_time as               usage_ts,
      dialect as                    usage_database_type,
      explore as                    usage_subject_area,
      issuer_source as              usage_originator_type,
      name AS                       user_name,
      rebuild_pdts_yes_no_   as     usage_triggered_cache_reload,
      source                        as usage_category,
      status                        as usage_status,
      title                         as usage_feature_title,
      approximate_web_usage_in_minutes as usage_time_elapsed_mins,
      average_runtime_in_seconds as usage_response_time_secs
FROM source )
select * from renamed
