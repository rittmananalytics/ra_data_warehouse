{{ config(
  materialized = 'table'
) }}

with source as (
  select *,
  max(_sdc_sequence) over (partition by pk order by _sdc_sequence range between unbounded preceding and unbounded following) as max_sdc_sequence
  from {{ source('everhour', 'all_team_time') }}
),

team_members as (
  select
  --team_name,
  --team_id,
  --budget,
  daily_cost,
  role_name,
  team_member_id,
  daily_billable_rate,
  team_member_email
   from {{ ref('everhour_team_members')}}
),


--need to push most of this upstream to a staging model for everhour time
renamed as (
  select
    pk,
    --task,
    --TIMESTAMP(task.dueOn) as task_due_at,
    --task.number,
    --task.iteration as task_iteration,
    task_status,
    task_type,
    --SAFE_CAST(SUBSTR(task.parentId, 4) AS INT64) as task_parentId,
    --task.labels as task_labels,
    task_name,
    SUBSTR(task_id, 4) as task_id,
    TRIM(user_name) as user_name,
    user_id,
    TIMESTAMP(date) as activity_date,
    --timestamp_add(activity_date, interval time second) as task_ended_at,
    billable_time,
    --timerTime,
    time
  from source s
  where _sdc_sequence = max_sdc_sequence
),

dedupe as (
  select
    *,
    RANK() over (partition by pk order by activity_date) as rank
  from renamed
),

tasks as (
  select task_pk, task_id, video_id, project_gid, project_name from {{ ref('tasks') }}
),

joined as (
  select
    h.*,
    t.video_id,
    t.task_pk,
    t.project_gid,
    t.project_name,
    --m.team_name,
    --m.team_id,
    --m.budget,
    m.daily_cost,
    m.daily_billable_rate,
    m.role_name,
    m.team_member_id,
    m.team_member_email
  from dedupe h
  LEFT JOIN tasks t
  ON h.task_id = t.task_id
  LEFT JOIN team_members m
  ON h.user_id = m.team_member_id
  where h.rank = 1
)

select * EXCEPT(rank) from joined
