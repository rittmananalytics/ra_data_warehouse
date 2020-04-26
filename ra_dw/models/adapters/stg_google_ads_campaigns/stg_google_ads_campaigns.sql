{% if not var("enable_google_ads_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
  {{ filter_source('stitch_google_ads','s_campaigns','id') }}
),
renamed as (

    select
        settings              as campaign_settings,
        startdate             as campaign_start_date,
        id,
        name,
        campaigntrialtype,
        status,
        servingstatus,
        basecampaignid,
        adservingoptimizationstatus,
        advertisingchanneltype,
        _sdc_customer_id,
        enddate

    from source

)

select * from renamed
