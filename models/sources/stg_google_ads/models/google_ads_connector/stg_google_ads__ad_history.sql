{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'google_ads' in var("marketing_warehouse_ad_sources") %}
{% if var("google_ads_api_source") == 'google_ads' %}


with base AS (

    SELECT *
    FROM {{ ref('stg_google_ads__ad_history_tmp') }}

),

fields AS (

    SELECT
        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('stg_google_ads__ad_history_tmp')),
                staging_columns=get_google_ads_ad_history_columns()
            )
        }}

    FROM base
),

final AS (

    SELECT
        ad_group_id,
        id AS ad_id,
        updated_at AS updated_timestamp,
        _fivetran_synced,
        ad_type,
        status AS ad_status
    FROM fields
),

most_recent AS (

    SELECT
        *,
        row_number() over (PARTITION BYad_id order by updated_timestamp desc) = 1 AS is_most_recent_record
    FROM final

)

SELECT * FROM most_recent

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
