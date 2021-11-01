with adapter as (

    select *
    from {{ ref('facebook_ads__ad_adapter') }}

), aggregated as (

    select
        date_day,
        account_id,
        account_name,
        campaign_id,
        campaign_name,
        sum(clicks) as clicks,
        sum(impressions) as impressions,
        sum(spend) as spend
    from adapter
    {{ dbt_utils.group_by(5) }}

)

select *
from aggregated