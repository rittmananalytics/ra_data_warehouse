{% if not var("enable_harvest_projects_source") or not var("enable_projects_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with sde_projects_ds_merge_list as
  (
    SELECT *
    FROM   {{ ref('sde_harvest_projects_projects') }}
  )
select * from sde_projects_ds_merge_list
