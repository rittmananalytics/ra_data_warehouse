{% if not var("enable_gamstop_source") %}
  {{
      config(
          enabled=false
      )
  }}
{% endif %}

with source as (

  select * from {{ source('gamstop','s_gamstop_check_history' ) }}
  where _fivetran_deleted is false

),

renamed as (

  select

    id as gamstop_natural_key,
    userid as platform_users_natural_key,
    checkcount as check_count,
    cast (createdat as timestamp) as gamstop_created_ts,
    cast (updatedat as timestamp) as gamstop_updated_ts,
    lower (gamstopstatus) as gamstop_status,
    lower (latestchecktype) as latest_check_type

  from source

),

valid_dates as (

  select

    *,
    gamstop_created_ts as gamstop_valid_from_ts,
    lead(gamstop_created_ts) over(partition by platform_users_natural_key order by gamstop_updated_ts) as gamstop_valid_to_ts,
    lag (gamstop_status) over (partition by platform_users_natural_key order by gamstop_updated_ts) as previous_gamstop_status

  from renamed

)

select * from valid_dates
