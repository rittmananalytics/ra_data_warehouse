{% if not var("enable_jira_projects_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with source as (
  SELECT
    * EXCEPT (_sdc_batched_at, max_sdc_batched_at)
  FROM (
    SELECT
      *,
      MAX(_sdc_batched_at)
       OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
       AS max_sdc_batched_at
    FROM
      {{ target.database}}.{{ var('stitch_issues_table') }})
  WHERE
    max_sdc_batched_at = _sdc_batched_at
),
renamed as (
select concat('{{ var('id-prefix') }}',id) as task_id,
       concat('{{ var('id-prefix') }}',fields.project.id) as project_id,
       concat('{{ var('id-prefix') }}',fields.reporter.key) as task_creator_user_id,
       fields.summary as task_name,
       fields.priority.name as task_priority,
       fields.issuetype.name as task_type,
       cast(null as string) as task_description,
       fields.status.statuscategory.name as task_status,
       cast(null as boolean) as task_is_completed,
       cast(null as timestamp)  as task_completed_ts,
       fields.created  as task_created_ts,
       fields.updated as task_last_modified_ts,
 from source)
SELECT
 *
FROM
 renamed
