{% if not var("enable_harvest_projects_source") or not var("enable_projects_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with t_projects_merge_list as
  (
    SELECT *
    FROM   {{ ref('stg_harvest_projects_projects') }}
  )
select * from t_projects_merge_list
