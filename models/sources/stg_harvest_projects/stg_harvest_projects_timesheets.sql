{% if not var("enable_harvest_projects_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with t_harvest_time_entries as (
  {{ filter_stitch_table(var('time_entries_table'),'id') }}

),
t_harvest_projects as (
    SELECT
      *
    FROM (
      SELECT
        *,
        MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
      FROM
        {{ target.database}}.{{ var('projects_table') }})
    WHERE
      max_sdc_batched_at = _sdc_batched_at
  ),
t_harvest_users_project_tasks as (
    SELECT
        *
    FROM (
        SELECT
            *,
             MAX(_sdc_batched_at) OVER (PARTITION BY project_task_id,user_id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
        FROM
            {{ target.database}}.{{ var('user_project_tasks_table') }}
        )
    WHERE
        _sdc_batched_at = max_sdc_batched_at
  ),
t_harvest_project_tasks as (
    SELECT
      *
    FROM (
      SELECT
        *,
        MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
      FROM
        {{ target.database}}.{{ var('project_tasks_table') }}
      )
    WHERE
      _sdc_batched_at = max_sdc_batched_at
  ),
t_harvest_tasks as (
    SELECT
        *
    FROM (
        SELECT
            *,
             MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
        FROM
            {{ target.database}}.{{ var('tasks_table') }}
        )
    WHERE
        _sdc_batched_at = max_sdc_batched_at
  ),
t_harvest_users as (
   SELECT
       *
   FROM (
       SELECT
           *,
            MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
       FROM
           {{ target.database}}.{{ var('users_table') }}
       )
   WHERE
       _sdc_batched_at = max_sdc_batched_at
 ),
renamed as (
SELECT
  concat('harvest-',t.client_id)               as company_id,
  cast(t.id as string)      as timesheet_id,
  concat('harvest-',t.user_id) as timesheet_users_id,
  t.project_id              as timesheet_project_id,
  t.task_assignment_id      as timesheet_task_assignment_id,
  ht.id                     as timesheet_task_id,
  t.invoice_id              as timesheet_invoice_id,
  t.spent_date              as timesheet_billing_date,
  t.hours                   as timesheet_hours_billed,
  case when t.is_billed then t.billable_rate * t.hours else 0 end as timesheet_total_amount_billed,
  t.billable                as timesheet_is_billable,
  t.is_billed               as timesheet_has_been_billed,
  t.is_locked               as timesheet_has_been_locked,
  t.billable_rate           as timesheet_billable_hourly_rate_amount,
  t.cost_rate               as timesheet_billable_hourly_cost_amount,
  t.notes                   as timesheet_notes
FROM
  t_harvest_time_entries t
  join t_harvest_projects p on t.project_id = p.id
  join t_harvest_users_project_tasks upt on t.task_assignment_id = upt.project_task_id and upt.user_id = t.user_id
  join t_harvest_project_tasks pt on upt.project_task_id = pt.id
  join t_harvest_tasks ht on pt.task_id = ht.id
  join t_harvest_users u on t.user_id = u.id
  {{ dbt_utils.group_by(n=16) }})
SELECT
    *
  FROM
    renamed
