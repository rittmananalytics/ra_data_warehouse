{{ config(enabled=var('ad_reporting__linkedin_ads_enabled')) }}

with base as (

    select *
    from {{ ref('linkedin__ad_adapter')}}

), fields as (

    select
        'LinkedIn Ads' as platform,
        cast(date_day as date) as date_day,
        account_name,
        cast(account_id as {{ dbt_utils.type_string() }}) as account_id,
        campaign_group_name as campaign_name,
        cast(campaign_group_id as {{ dbt_utils.type_string() }}) as campaign_id,
        campaign_name as ad_group_name,
        cast(campaign_id as {{ dbt_utils.type_string() }}) as ad_group_id,
        base_url,
        url_host,
        url_path,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        coalesce(clicks, 0) as clicks,
        coalesce(impressions, 0) as impressions,
        coalesce(cost, 0) as spend
    from base


)

select *
from fields
