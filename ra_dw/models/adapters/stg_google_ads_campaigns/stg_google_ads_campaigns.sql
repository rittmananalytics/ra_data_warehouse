{% if not var("enable_google_ads_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
  SELECT
    * EXCEPT (_sdc_batched_at, max_sdc_batched_at)
  FROM
    (
      SELECT
        *,
        MAX(_sdc_batched_at) OVER (PARTITION BY gid ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
      FROM
        {{ source('stitch_google_ads','s_campaigns') }}
    )
  WHERE
    _sdc_batched_at = max_sdc_batched_at
),

renamed as (

    select
        settings              as campaign_settings,
        startdate             as campaign_start_date,
        id,
        name,
        campaigntrialtype,
        _sdc_table_version,
        status,
        servingstatus,
        basecampaignid,
        _sdc_received_at,
        _sdc_sequence,
        adservingoptimizationstatus,
        advertisingchanneltype,
        _sdc_customer_id,
        _sdc_batched_at,
        _sdc_extracted_at,
        enddate

    from source

)

select * from renamed
