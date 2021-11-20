{% if target.type == 'bigquery' %}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}

with base AS (

    SELECT *
    FROM {{ ref('int__facebook_ads__carousel_media_prep') }}

), unnested AS (

    SELECT
        _fivetran_id,
        creative_id,
        index,
        json_extract_scalar(element, '$.key') AS key,
        json_extract_scalar(element, '$.value') AS value
    FROM base
    inner join unnest(url_tags) AS element

)

SELECT *
FROM unnested

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
