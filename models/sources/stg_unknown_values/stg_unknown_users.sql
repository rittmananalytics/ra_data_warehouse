SELECT
  'Unassigned'            as user_id,
  'Unassigned'             as user_name  ,
  'Unassigned'        as user_email,
  FALSE                      as user_is_contractor,
  false  as user_is_staff,
  0           as user_weekly_capacity,
  cast(null as string)           as user_phone,
  0           as user_default_hourly_rate,
  0           as user_cost_rate,
  false                        as user_is_active,
  cast(null as timestamp)        as user_created_ts,
  cast(null as timestamp)       as user_last_modified_ts
