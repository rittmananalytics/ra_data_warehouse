{% if not enable_asana_projects and not enable_jira_projects%}
{{
    config(
        enabled=false
    )
}}
with sde_tasks_ds_merge_list as
  (
    {% if enable_jira_projects %}
    SELECT *
    FROM   {{ ref('sde_jira_projects_tasks') }}
    {% endif %}
    {% if enable_jira_projects and enable_jira_projects %}
    UNION ALL
    {% endif %}
    {% if enable_asana_projects %}
    SELECT *
    FROM   {{ ref('sde_asana_projects_tasks') }}
    {% endif %}
  )
select * from sde_tasks_ds_merge_list
