{% if (not var("enable_asana_projects_source") and not var("enable_jira_projects_source")) or not var("enable_projects_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
with t_delivery_projects_merge_list as
  (
    SELECT *
    FROM   {{ ref('stg_jira_projects_projects') }}


    {% if enable_asana_projects_source %}
    UNION ALL
    SELECT *
    FROM   {{ ref('stg_asana_projects_projects') }}
    {% endif %}
  )
select * from t_delivery_projects_merge_list
