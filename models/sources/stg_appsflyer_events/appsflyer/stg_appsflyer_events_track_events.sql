{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("product_warehouse_event_sources") %}
{% if 'appsflyer_events_track' in var("product_warehouse_event_sources") %}

with source as (

  select * from {{ source('appsflyer', 'tracks') }}

),

renamed as (

select

    cast(appsflyer_id as {{ dbt_utils.type_string() }})           as event_id,
    cast(event_name as {{ dbt_utils.type_string() }})             as event_type,
    event_time                                                    as event_ts,
    cast( event_value as {{ dbt_utils.type_string() }})           as event_details,
    cast(app_name as {{ dbt_utils.type_string() }})               as page_title,
    cast(null as {{ dbt_utils.type_string() }})                   as page_url_path,
    cast(http_referrer as {{ dbt_utils.type_string() }})          as referrer_host,
    cast(null as {{ dbt_utils.type_string() }})                   as search,
    cast(original_url as {{ dbt_utils.type_string() }})           as page_url,
    cast(null as {{ dbt_utils.type_string() }})                   as page_url_host,
    cast(null as {{ dbt_utils.type_string() }})                   as gclid,
    cast(af_keywords as {{ dbt_utils.type_string() }})            as utm_term,
    cast(af_adset as {{ dbt_utils.type_string() }})               as utm_content,
    cast(af_ad_type as {{ dbt_utils.type_string() }})             as utm_medium,
    cast(campaign as {{ dbt_utils.type_string() }})               as utm_campaign,
    cast(media_source as {{ dbt_utils.type_string() }})           as utm_source,
    cast(ip as {{ dbt_utils.type_string() }})                     as ip,
    cast(idfa as {{ dbt_utils.type_string() }})                   as visitor_id,
    cast(customer_user_id as {{ dbt_utils.type_string() }})       as user_id,
    case
        when lower(user_agent) like '%android%' then 'Android'
        else replace(
          split(user_agent,'(')[safe_offset(1)],
            ';', '')
    end                                                           as device,
    cast(af_siteid as {{ dbt_utils.type_string() }})              as site
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
