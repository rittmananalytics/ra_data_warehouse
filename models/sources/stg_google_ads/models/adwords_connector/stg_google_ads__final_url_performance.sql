{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'google_ads' in var("marketing_warehouse_ad_sources") %}
{% if var("google_ads_api_source") == 'adwords' %}

with source AS (

    SELECT *
    FROM {{ ref('stg_google_ads__final_url_performance_tmp') }}

),

renamed AS (

    SELECT

        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('stg_google_ads__final_url_performance_tmp')),
                staging_columns=get_google_ads_final_url_performance_columns()
            )
        }}

        {% for metric in [] %}
        , {{ metric }}
        {% endfor %}

    FROM source

),

url_fields AS (

    SELECT
        *,
        {{ dbt_utils.split_part('final_url', "'?'", 1) }} AS base_url,
        {{ dbt_utils.get_url_host('final_url') }} AS url_host,
        '/' || {{ dbt_utils.get_url_path('final_url') }} AS url_path,

        {% if var('google_auto_tagging_enabled', false) %}

        coalesce( {{ dbt_utils.get_url_parameter('final_url', 'utm_source') }} , 'google')  AS utm_source,
        coalesce( {{ dbt_utils.get_url_parameter('final_url', 'utm_medium') }} , 'cpc') AS utm_medium,
        coalesce( {{ dbt_utils.get_url_parameter('final_url', 'utm_campaign') }} , campaign_name) AS utm_campaign,
        coalesce( {{ dbt_utils.get_url_parameter('final_url', 'utm_content') }} , ad_group_name) AS utm_content,

        {% else %}

        {{ dbt_utils.get_url_parameter('final_url', 'utm_source') }} AS utm_source,
        {{ dbt_utils.get_url_parameter('final_url', 'utm_medium') }} AS utm_medium,
        {{ dbt_utils.get_url_parameter('final_url', 'utm_campaign') }} AS utm_campaign,
        {{ dbt_utils.get_url_parameter('final_url', 'utm_content') }} AS utm_content,

        {% endif %}

        {{ dbt_utils.get_url_parameter('final_url', 'utm_term') }} AS utm_term

    FROM renamed

), surrogate_key AS (

    SELECT
        *,
        {{ dbt_utils.surrogate_key(['date_day','campaign_id','ad_group_id','final_url']) }} AS final_url_performance_id
    FROM url_fields

)

SELECT * FROM surrogate_key

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
