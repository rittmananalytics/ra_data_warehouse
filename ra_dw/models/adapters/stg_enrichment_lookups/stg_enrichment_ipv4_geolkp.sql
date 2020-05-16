{% if not var("enable_geolite2_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
    select *
    from
    {{ source('geolite2','s_ipv4_city_blocks' ) }}
), geolite2 as
    select *
    from
    {{ ref( 'stg_custom_geolite2_city_blocks' ) }}
renamed as
(
select  concat('geolite2-',geoname_id)  as geoname_id,
        network                         as network,
        registered_country_geoname_id   as registered_country_geoname_id,
        represented_country_geoname_id  as represented_country_geoname_id,
        is_anonymous_proxy              as is_anonymous_proxy,
        is_satellite_provider           as is_satellite_provider,
        postal_code                     as        postal_code,
        latitude                        as        latitude,
        longitude                       as        longitude,
        accuracy_radius                 as        accuracy_radius,
        center_point                    as        center_point
from source
)
select * from renamed
