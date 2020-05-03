{% if not var("enable_facebook_ads_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
  {{ filter_source('stitch_facebook_ads','s_adsets','id') }}

),

renamed as (

    select
        concat('facebook-ads-',id) as adset_id,
        name as adset_name,
        concat('facebook-ads-',account_id) as account_id,
        concat('facebook-ads-',campaign_id) as campaign_id,
        budget_remaining as adset_budget_remaining,
        effective_status as adset_effective_status,
        targeting as adset_targeting,
        created_time as adset_created_ts,
        end_time as adset_end_ts,
        start_time as adset_start_ts,
        updated_time as adset_last_modified_ts
    from source
)

select * from renamed
