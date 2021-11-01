{{ config(enabled=var('api_source') == 'adwords') }}

with base as (

    select *
    from {{ var('click_performance') }}

), fields as (

    select
        date_day,
        campaign_id,
        ad_group_id,
        criteria_id,
        gclid,
        row_number() over (partition by gclid order by date_day) as rn
    from base

), filtered as ( -- we've heard that sometimes duplicates gclids are an issue. This dedupe ensures no glcids are double counted.

    select *
    from fields
    where gclid is not null 
    and rn = 1

)

select * from filtered