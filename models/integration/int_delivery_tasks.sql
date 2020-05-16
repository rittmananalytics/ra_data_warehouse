{% if (not var("enable_asana_projects_source") and not var("enable_jira_projects_source")) or not var("enable_projects_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
with t_tasks_merge_list as
  (
    {% if var("enable_jira_projects_source") %}
    SELECT *
    FROM   {{ ref('stg_jira_projects_tasks') }}
    {% endif %}
    {% if var("enable_jira_projects_source") and var("enable_asana_projects_source") %}
    UNION ALL
    {% endif %}
    {% if var("enable_asana_projects_source") %}
    SELECT *
    FROM   {{ ref('stg_asana_projects_tasks') }}
    {% endif %}
  )
select * from t_tasks_merge_list
