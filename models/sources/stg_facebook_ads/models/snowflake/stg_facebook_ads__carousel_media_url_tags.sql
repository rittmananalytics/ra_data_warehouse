{% if target.type == 'snowflake' %}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}

with base AS (

    SELECT *
    FROM {{ ref('int__facebook_ads__carousel_media_prep') }}

), unnested AS (

    SELECT

        base._fivetran_id,
        base.creative_id,
        base.index,
        url_tags.value:key::string AS key,
        url_tags.value:value::string AS value

    FROM base,
    lateral flatten( input => url_tags ) AS url_tags

)

SELECT *
FROM unnested

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
