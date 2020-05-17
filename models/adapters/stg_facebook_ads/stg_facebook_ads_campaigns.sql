{% if not var("enable_facebook_ads_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
  {{ filter_stitch_table(var('campaigns_table'),'id') }}

),

renamed as (

    select
        concat('facebook-ads-',id)      as campaign_id,
        name      as campaign_name,
        concat('facebook-ads-',account_id) as account_id,
        objective as campaign_objective,
        effective_status as campaign_effective_status,
        buying_type as campaign_buying_type,
        start_time as campaign_start_ts,
        updated_time as campaign_last_modified_ts
    from source

)

select * from renamed
