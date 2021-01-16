{% if not var("enable_kyc_source") %}
  {{
      config(
          enabled=false
      )
  }}
{% endif %}

with source as (

  select * from {{ source('kyc_checks','s_kyc_check_results' ) }}
  where _fivetran_deleted is false

),

renamed as (

  select

   cast (id as numeric) as kyc_check_result_id_natural_key,
   cast (kyc_check_id as numeric) as kyc_check_id_natural_key,
   cast(started_time as timestamp) as results_started_ts,
   cast(completed_time as timestamp) as results_completed_ts,
   lower(report_name) as kyc_report_name,
   lower (result_state) as kyc_result_state,
   lower (comments) as result_comment

  from source

)

select * from renamed
