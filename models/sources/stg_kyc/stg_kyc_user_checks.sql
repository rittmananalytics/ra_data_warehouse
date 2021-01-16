{% if not var("enable_kyc_source") %}
  {{
      config(
          enabled=false
      )
  }}
{% endif %}

with source as (

  select * from {{ source('kyc_checks','s_kyc_checks' ) }}
  where _fivetran_deleted is false

),

renamed as (

  select

    cast (id as int64) as kyc_check_id_natural_key,
    cast (owner_id as int64) as kyc_user_id_natural_key,
    cast(started_time as timestamp) as check_started_ts,
    cast(completed_time as timestamp) as check_completed_ts,
    lower (check_state) as kyc_check_state,
    lower (check_type) as kyc_check_type,
    lower (provider_source) as kyc_provider_source

  from source

)

select * from renamed
