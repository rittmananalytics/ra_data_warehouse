{% if not var("enable_kyc_source") or not var("enable_platform_warehouse") %}
  {{
      config(
          enabled=false
      )
  }}
  {% endif %}

  with user_check_results as (

    select * from {{ ref('stg_kyc_user_check_results') }}

  ),

  user_checks as (

    select * from {{ ref('stg_kyc_user_checks') }}

  ),

  users as (

    select * from {{ ref('stg_kyc_users') }}

  ),

  merge_sources as (

    select * from user_check_results
    left join user_checks using (kyc_check_id_natural_key)
    left join users using (kyc_user_id_natural_key)

  ),

  event_index as (

    select

    *,
    results_started_ts as kyc_valid_from_ts,
    lead(results_started_ts) over(partition by kyc_user_id_natural_key order by results_started_ts) as kyc_valid_to_ts,
    row_number () over (partition by kyc_user_id_natural_key order by min(results_started_ts)) as check_result_sequence_for_user
    
    from merge_sources

    {{ dbt_utils.group_by(n=17) }}

  )

select * from event_index
