{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'google_ads' in var("marketing_warehouse_ad_sources") %}
{% if var("google_ads_api_source") == 'google_ads' %}

with base AS (

    SELECT *
    FROM {{ ref('stg_google_ads__ad_final_url_history_tmp') }}

),

fields AS (

    SELECT
        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('stg_google_ads__ad_final_url_history_tmp')),
                staging_columns=get_google_ads_ad_final_url_history_columns()
            )
        }}

    FROM base
),

final AS (

    SELECT
        ad_group_id,
        ad_id,
        updated_at AS updated_timestamp,
        _fivetran_synced,
        url AS final_url
    FROM fields
),

most_recent AS (

    SELECT
        *,
        row_number() over (PARTITION BYad_id order by updated_timestamp desc) = 1 AS is_most_recent_record
    FROM final

),

url_fields AS (

    SELECT
        *,
        {{ dbt_utils.split_part('final_url', "'?'", 1) }} AS base_url,
        {{ dbt_utils.get_url_host('final_url') }} AS url_host,
        '/' || {{ dbt_utils.get_url_path('final_url') }} AS url_path,
        {{ dbt_utils.get_url_parameter('final_url', 'utm_source') }} AS utm_source,
        {{ dbt_utils.get_url_parameter('final_url', 'utm_medium') }} AS utm_medium,
        {{ dbt_utils.get_url_parameter('final_url', 'utm_campaign') }} AS utm_campaign,
        {{ dbt_utils.get_url_parameter('final_url', 'utm_content') }} AS utm_content,
        {{ dbt_utils.get_url_parameter('final_url', 'utm_term') }} AS utm_term
    FROM most_recent

)

SELECT * FROM url_fields

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
