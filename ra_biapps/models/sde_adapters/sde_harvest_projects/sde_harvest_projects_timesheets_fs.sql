{{
    config(
        materialized='table'
    )
}}
with harvest_time_entries as (
  SELECT
      *
  FROM (
      SELECT
          *,
          MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_sdc_batched_at
      FROM
          {{ source('harvest', 'time_entries') }}
      )
  WHERE
      _sdc_batched_at = latest_sdc_batched_at
  {{ dbt_utils.group_by(n=29) }}
),
  harvest_projects as (
    SELECT
      *
    FROM (
      SELECT
        *,
        MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_sdc_batched_at
      FROM
        {{ source('harvest', 'projects') }})
    WHERE
      latest_sdc_batched_at = _sdc_batched_at
  ),
  harvest_user_project_tasks as (
    SELECT
        *
    FROM (
        SELECT
            *,
             MAX(_sdc_batched_at) OVER (PARTITION BY project_task_id,user_id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_sdc_batched_at
        FROM
            {{ source('harvest', 'user_project_tasks') }}
        )
    WHERE
        _sdc_batched_at = latest_sdc_batched_at
  ),
  harvest_project_tasks as (
    SELECT
      *
    FROM (
      SELECT
        *,
        MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_sdc_batched_at
      FROM
        {{ source('harvest', 'project_tasks') }}
      )
    WHERE
      _sdc_batched_at = latest_sdc_batched_at
  ),
  harvest_tasks as (
    SELECT
        *
    FROM (
        SELECT
            *,
             MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_sdc_batched_at
        FROM
            {{ source('harvest', 'tasks') }}
        )
    WHERE
        _sdc_batched_at = latest_sdc_batched_at
  ),
 harvest_users as (
   SELECT
       *
   FROM (
       SELECT
           *,
            MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_sdc_batched_at
       FROM
           {{ source('harvest', 'users') }}
       )
   WHERE
       _sdc_batched_at = latest_sdc_batched_at
 )
SELECT
  t.id,
  t.spent_date,
  t.user_id,
  t.project_id,
  t.client_id,
  t.invoice_id,
  t.billable,
  t.is_billed,
  t.is_locked,
  t._sdc_batched_at,
  t._sdc_sequence,
  t.billable_rate,
  t.cost_rate,
  t.notes,
  t.hours,
  t.billable_rate * t.hours as billable_revenue,
  case when t.is_billed then t.billable_rate * t.hours end as billed_revenue,
  t.task_assignment_id,
  ht.id as task_id
FROM
  harvest_time_entries t
  harvest_projects p on t.project_id = p.id
  harvest_user_project_tasks upt on t.task_assignment_id = upt.project_task_id and upt.user_id = t.user_id
  harvest_project_tasks pt on upt.project_task_id = pt.id
  harvest_tasks ht on pt.task_id = ht.id
  harvest_users u on t.user_id = u.id
