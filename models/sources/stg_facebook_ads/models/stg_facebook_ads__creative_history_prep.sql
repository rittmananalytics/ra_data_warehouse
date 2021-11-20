{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}

{% set url_field = "coalesce(page_link,template_page_link)" %}

with base AS (

    SELECT *
    FROM {{ ref('stg_facebook_ads__creative_history') }}
    where is_most_recent_record = true

), url_tags AS (

    SELECT *
    FROM {{ ref('stg_facebook_ads__url_tag') }}

), url_tags_pivoted AS (

    SELECT
        _fivetran_id,
        creative_id,
        min(case when key = 'utm_source' then value end) AS utm_source,
        min(case when key = 'utm_medium' then value end) AS utm_medium,
        min(case when key = 'utm_campaign' then value end) AS utm_campaign,
        min(case when key = 'utm_content' then value end) AS utm_content,
        min(case when key = 'utm_term' then value end) AS utm_term
    FROM url_tags
    group by 1,2

), fields AS (

    SELECT
        _fivetran_id,
        creative_id,
        account_id,
        creative_name,
        {{ url_field }} AS url,
        {{ dbt_utils.split_part(url_field, "'?'", 1) }} AS base_url,
        {{ dbt_utils.get_url_host(url_field) }} AS url_host,
        '/' || {{ dbt_utils.get_url_path(url_field) }} AS url_path,
        coalesce(url_tags_pivoted.utm_source, {{ dbt_utils.get_url_parameter(url_field, 'utm_source') }}) AS utm_source,
        coalesce(url_tags_pivoted.utm_medium, {{ dbt_utils.get_url_parameter(url_field, 'utm_medium') }}) AS utm_medium,
        coalesce(url_tags_pivoted.utm_campaign, {{ dbt_utils.get_url_parameter(url_field, 'utm_campaign') }}) AS utm_campaign,
        coalesce(url_tags_pivoted.utm_content, {{ dbt_utils.get_url_parameter(url_field, 'utm_content') }}) AS utm_content,
        coalesce(url_tags_pivoted.utm_term, {{ dbt_utils.get_url_parameter(url_field, 'utm_term') }}) AS utm_term
    FROM base
    left join url_tags_pivoted
        using (_fivetran_id, creative_id)

)

SELECT *
FROM fields

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
