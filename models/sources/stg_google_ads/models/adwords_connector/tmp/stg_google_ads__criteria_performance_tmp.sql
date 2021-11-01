{{ config(enabled=var('google_ads_api_source') == 'adwords') }}

select *
from {{ source('adwords','criteria_performance') }}
