{%- macro geo_located() -%}

geo_located as (
  SELECT * except(network_bin, mask,geoname_id,registered_country_geoname_id,represented_country_geoname_id,is_anonymous_proxy)
  FROM (
    SELECT *, NET.SAFE_IP_FROM_STRING(ip) & NET.IP_NET_MASK(4, mask) network_bin
    FROM ordered, UNNEST(GENERATE_ARRAY(9,32)) mask
    WHERE BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(ip)) = 4
  )
  JOIN `fh-bigquery.geocode.201806_geolite2_city_ipv4_locs`
  USING (network_bin, mask)
)
{%- endmacro -%}
