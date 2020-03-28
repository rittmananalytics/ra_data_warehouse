{{
    config(
        unique_key='staff_pk',
        alias='staff_dim'
    )
}}
WITH unique_staff AS
  (
  SELECT lower(staff_full_name) as staff_full_name
  FROM   {{ ref('sde_staff_ds') }}
  GROUP BY 1
  )
,
  unique_staff_with_uuid AS
  (
  SELECT staff_full_name,
         GENERATE_UUID() as staff_uid
  FROM   unique_staff
  )

SELECT
   s.source,
   GENERATE_UUID() as staff_pk,
   s.staff_id,
   s.staff_full_name,
   s.staff_email,
   s.staff_is_contractor,
   s.staff_weekly_capacity,
   s.staff_phone,
   s.staff_default_hourly_rate,
   s.staff_cost_rate,
   s.staff_is_active
FROM
   {{ ref('sde_staff_ds') }} s
JOIN unique_staff_with_uuid  u
ON lower(s.staff_full_name) = u.staff_full_name
