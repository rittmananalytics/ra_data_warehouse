{% if not var("enable_looker_usage_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
WITH source AS (

    select
      *
    from
      `ra-development.fivetran_email.usage_stats`

),
renamed as (
  SELECT
    pk,
    concat('looker-',client) as company_id,
    history_created_time created_time,
    history_dialect dialect,
    history_id id,
    history_issuer_source issuer_source,
    history_rebuild_pdts_yes_no_ rebuild_pdts_yes_no_,
    history_status status,
    look_title title,
    query_explore explore,
    user_name name,
    history_approximate_web_usage_in_minutes approximate_web_usage_in_minutes,
    history_average_runtime_in_seconds average_runtime_in_seconds,
    dashboard_title
  FROM
    source
  where history_id is not null
  union all
  SELECT
    pk,
    concat('looker-',client) as company_id,
    created_time,
    dialect,
    id,
    issuer_source,
    rebuild_pdts_yes_no_,
    status,
    title,
    explore,
    name,
    approximate_web_usage_in_minutes,
    safe_cast(average_runtime_in_seconds as float64) as average_runtime_in_seconds,
    null as dashboard_title
  FROM
    source
  where id is not null)
select * from renamed
