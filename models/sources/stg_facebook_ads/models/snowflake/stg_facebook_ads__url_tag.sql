{% if target.type == 'snowflake' %}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}

with base AS (

    SELECT *
    FROM {{ ref('stg_facebook_ads__creative_history') }}

), required_fields AS (

    SELECT
        _fivetran_id,
        creative_id,
        parse_json(url_tags) AS url_tags
    FROM base
    where url_tags is not null


), flattened_url_tags AS (

    SELECT
        _fivetran_id,
        creative_id,
        url_tags.value:key::string AS key,
        url_tags.value:value::string AS value,
        url_tags.value:type::string AS type
    FROM required_fields,
    lateral flatten( input => url_tags ) AS url_tags


)

SELECT *
FROM flattened_url_tags

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
