with source as (
  select
    *,
    max(_sdc_sequence) over (partition by id order by _sdc_sequence range between unbounded preceding and unbounded following) as max_sdc_sequence
  from {{ source('everhour', 'task_stages') }}
),

renamed as (
  select
    SUBSTR(id, 4) as task_id,
    iteration as task_stage,
    REGEXP_EXTRACT(iteration, r'^( ?[0-9\.]+)\.? ?.*') as  stage_order
    --case when blog = '' then null else blog end as blog
  from source s
  where _sdc_sequence = max_sdc_sequence
)

select task_id, max(task_stage) as task_stage, max(stage_order) as stage_order from renamed group by task_id
