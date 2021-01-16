{% if not var("enable_affordability_source") %}
  {{
      config(
          enabled=false
      )
  }}
{% endif %}

with source as (

  select * from {{ source('affordability_checks','s_affordability_checks' ) }}
  where _fivetran_deleted is null

),

renamed as (

  select

    cast (id as int64) as affordability_natural_key,
    check_id as affordability_check_natural_key,
    request_id as request_natural_key,
    cast (user_id as int64) as platform_users_natural_key,
    cast (check_time as timestamp) as check_ts,
    cast (created_on as timestamp) as affordability_created_ts,
    lower (score) as affordability_score

  from source
),

valid_dates as (

  select

    *,
    check_ts as affordability_valid_from_ts,
    lead(check_ts) over(partition by platform_users_natural_key order by check_ts) as affordability_valid_to_ts,
    lag (affordability_score) over (partition by platform_users_natural_key order by check_ts) as previous_affordability_score

  from renamed

)

select * from valid_dates
