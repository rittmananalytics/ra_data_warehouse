{% if (not var("enable_jira_projects_source") and not var("enable_asana_projects_source") and not var("enable_harvest_projects_source")) or not var("enable_projects_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with t_users_merge_list as
  (
    {% if var("enable_harvest_projects_source") %}
    SELECT *
    FROM   {{ ref('stg_harvest_projects_users') }}
    {% endif %}

    {% if var("enable_jira_projects_source") and var("enable_harvest_projects_source") %}
    UNION ALL
    {% endif %}

    {% if var("enable_jira_projects_source") %}
    SELECT *
    FROM   {{ ref('stg_jira_projects_users') }}
    {% endif %}

    {% if (var("enable_harvest_projects_source") or var("enable_jira_projects_source")) and var("enable_asana_projects_source") %}
    UNION ALL
    {% endif %}

    {% if var("enable_asana_projects_source") %}
    SELECT *
    FROM   {{ ref('stg_asana_projects_users') }}
    UNION ALL
    {% endif %}

    SELECT *
    FROM   {{ ref('stg_unknown_users') }}
  )
,
 user_emails as (
       SELECT user_name, array_agg(distinct lower(user_email) ignore nulls) as all_user_emails
       FROM t_users_merge_list
       group by 1),
 user_ids as (
       SELECT user_name, array_agg(user_id ignore nulls) as all_user_ids
       FROM t_users_merge_list
       group by 1)
 select i.all_user_ids,
        u.*,
        e.all_user_emails
       from (
select user_name,
max(user_is_contractor) as user_is_contractor,
max(user_is_staff) as user_is_staff,
max(user_weekly_capacity) as user_weekly_capacity ,
min(user_phone) as user_phone,
max(user_default_hourly_rate) as user_default_hourly_rate,
max(user_cost_rate) as user_cost_rate,
max(user_is_active) as user_is_active,
min(user_created_ts) as user_created_ts,
max(user_last_modified_ts) as user_last_modified_ts,
FROM t_users_merge_list
group by 1) u
join user_emails e on u.user_name = coalesce(e.user_name,'Unknown')
join user_ids i on u.user_name = i.user_name
