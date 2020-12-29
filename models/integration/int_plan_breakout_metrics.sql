{{
    config(
        enabled=false
    )
}}


with plans_breakout_merge_list as
  (
    SELECT *
    FROM   {{ ref('stg_baremetrics_plan_breakout') }}
  )
select * from plans_breakout_merge_list
