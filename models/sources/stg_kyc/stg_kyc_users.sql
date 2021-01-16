{% if not var("enable_kyc_source") %}
  {{
      config(
          enabled=false
      )
  }}
{% endif %}

with source as (

  select * from {{ source('kyc_checks','s_kyc_users' ) }}
  where _fivetran_deleted is false

),

renamed as (

  select

   cast (user_id as int64) as platform_users_natural_key,
   cast (id as int64) as kyc_user_id_natural_key,
   cast (created_time as timestamp) as kyc_created_ts,
   lower (first_name) as first_name,
   lower (last_name) as last_name

  from source

)

select * from renamed
