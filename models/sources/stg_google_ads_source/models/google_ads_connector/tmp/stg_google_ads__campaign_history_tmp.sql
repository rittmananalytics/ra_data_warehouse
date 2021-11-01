{{ config(enabled=var('api_source') == 'google_ads') }}

select * from {{ var('google_ads__campaign_history') }}
