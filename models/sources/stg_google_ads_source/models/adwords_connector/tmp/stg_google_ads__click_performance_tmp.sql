{{ config(enabled=var('api_source') == 'adwords') }}

select *
from {{ var('google_ads__click_performance') }}