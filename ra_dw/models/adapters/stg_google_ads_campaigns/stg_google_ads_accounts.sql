{% if not var("enable_google_ads_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
  {{ filter_source('stitch_google_ads','s_accounts','customerid') }}
),

renamed as (

    select
        currencycode as account_currency_code,
        testaccount  as is_test_account,
        datetimezone as account_tz,
        customerid   as account_id,
        canmanageclients as is_client_manager

    from source

)

select * from renamed
