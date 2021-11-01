{{ config(enabled=var('google_ads_api_source') == 'google_ads') }}

select * from {{ source('adwords','account') }}
