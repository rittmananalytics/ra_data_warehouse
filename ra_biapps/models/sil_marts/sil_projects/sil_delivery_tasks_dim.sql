{{
    config(
        unique_key='delivery_task_pk',
        alias='delivery_tasks_dim'
    )
}}
WITH tasks AS
  (
  SELECT *
  FROM   {{ ref('sde_delivery_tasks_ds') }}
  )
SELECT
   GENERATE_UUID() as task_pk,
   t.*
FROM
   tasks t
