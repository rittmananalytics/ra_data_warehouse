{% macro get_ad_network_staging_files() %}

    {% set staging_file = [] %}

    {% if 'pinterest_ads' in var("marketing_warehouse_ad_sources") %}
    {% set _ = staging_file.append(ref('int_pinterest_ads')) %}
    {% endif %}

    {% if 'microsoft_ads' in var("marketing_warehouse_ad_sources") %}
    {% set _ = staging_file.append(ref('int_microsoft_ads')) %}
    {% endif %}

    {% if 'linkedin_ads' in var("marketing_warehouse_ad_sources") %}
    {% set _ = staging_file.append(ref('int_linkedin_ads')) %}
    {% endif %}

    {% if 'twitter_ads' in var("marketing_warehouse_ad_sources") %}
    {% set _ = staging_file.append(ref('int_twitter_ads')) %}
    {% endif %}

    {% if 'google_ads' in var("marketing_warehouse_ad_sources") %}
    {% set _ = staging_file.append(ref('int_google_ads')) %}
    {% endif %}

    {% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}
    {% set _ = staging_file.append(ref('int_facebook_ads')) %}
    {% endif %}

    {% if 'snapchat_ads' in var("marketing_warehouse_ad_sources") %}
    {% set _ = staging_file.append(ref('int_snapchat_ads')) %}
    {% endif %}


    {{ return(staging_file) }}

{% endmacro %}
