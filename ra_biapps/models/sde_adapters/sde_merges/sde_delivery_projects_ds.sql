{% if (not var("enable_asana_projects_source") and not var("enable_jira_projects_source")) or not var("enable_projects_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
with sde_delivery_projects_ds_merge_list as
  (
    SELECT *
    FROM   {{ ref('sde_jira_projects_projects') }}


    {% if enable_asana_projects_source %}
    UNION ALL
    SELECT *
    FROM   {{ ref('sde_asana_projects_projects') }}
    {% endif %}
  )
select * from sde_delivery_projects_ds_merge_list
