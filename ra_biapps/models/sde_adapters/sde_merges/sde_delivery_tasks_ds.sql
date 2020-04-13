{% if (not var("enable_asana_projects") and not var("enable_jira_projects")) or not var("enable_projects_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
with sde_tasks_ds_merge_list as
  (
    {% if var("enable_jira_projects") %}
    SELECT *
    FROM   {{ ref('sde_jira_projects_tasks') }}
    {% endif %}
    {% if var("enable_jira_projects") and var("enable_asana_projects") %}
    UNION ALL
    {% endif %}
    {% if var("enable_asana_projects") %}
    SELECT *
    FROM   {{ ref('sde_asana_projects_tasks') }}
    {% endif %}
  )
select * from sde_tasks_ds_merge_list
