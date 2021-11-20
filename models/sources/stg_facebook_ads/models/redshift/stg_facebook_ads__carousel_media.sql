{% if target.type == 'redshift' %}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}


with base AS (

    SELECT *
    FROM {{ ref('int__facebook_ads__carousel_media_prep') }}

), fields AS (

    SELECT
        _fivetran_id,
        creative_id,
        caption,
        description,
        message,
        link,
        index
    FROM base

)

SELECT *
FROM fields

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
