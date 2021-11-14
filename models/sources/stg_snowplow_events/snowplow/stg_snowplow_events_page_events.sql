{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("product_warehouse_event_sources") %}
{% if 'snowplow_events_page' in var("product_warehouse_event_sources") %}

with source as (

  select * from {{ source('snowplow', 'pages') }}

),

renamed as (

  SELECT
    cast(event_id as {{ dbt_utils.type_string() }})           as event_id,
    cast(event as {{ dbt_utils.type_string() }})              as event_type,
    dvce_created_tstamp                                       as event_ts,
    cast(page_title as {{ dbt_utils.type_string() }})         as event_details,
    cast(page_title as {{ dbt_utils.type_string() }})         as page_title,
    cast(page_urlpath as {{ dbt_utils.type_string() }})       as page_url_path,
    cast(refr_urlhost as {{ dbt_utils.type_string() }})       as referrer_host,
    cast(null as {{ dbt_utils.type_string() }})               as search,
    cast(page_url as {{ dbt_utils.type_string() }})           as page_url,
    cast(page_urlhost as {{ dbt_utils.type_string() }})       as page_url_host,
    cast(null as {{ dbt_utils.type_string() }})               as gclid,
    cast(mkt_term as {{ dbt_utils.type_string() }})           as utm_term,
    cast(mkt_content as {{ dbt_utils.type_string() }})        as utm_content,
    cast(mkt_medium as {{ dbt_utils.type_string() }})         as utm_medium,
    cast(mkt_campaign as {{ dbt_utils.type_string() }})       as utm_campaign,
    cast(mkt_source as {{ dbt_utils.type_string() }})         as utm_source,
    cast(user_ipaddress as {{ dbt_utils.type_string() }})     as ip,
    cast(coalesce(network_userid,domain_userid) as {{ dbt_utils.type_string() }}) as visitor_id,
    cast(user_id as {{ dbt_utils.type_string() }})            as user_id,
    case
        when lower(useragent) like '%android%' then 'Android'
        else replace(
          split(useragent,'(')[safe_offset(1)],
            ';', '')
    end                                                       as device,
    cast(page_urlhost as {{ dbt_utils.type_string() }})       as site
    FROM
      source
),

final as (

    select
        *,
        case
            when device = 'iPhone' then 'iPhone'
            when device = 'Android' then 'Android'
            when device in ('iPad', 'iPod') then 'Tablet'
            when device in ('Windows', 'Macintosh', 'X11') then 'Desktop'
            else 'Uncategorized'
        end as device_category
    from renamed

)

select * from final

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
