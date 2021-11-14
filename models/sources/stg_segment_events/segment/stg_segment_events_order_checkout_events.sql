{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("product_warehouse_event_sources") %}
{% if 'segment_events_order_checkout' in var("product_warehouse_event_sources") %}


with source as (

    select * from {{ source('segment', 'order_checkout') }}

),

renamed as (

    select
        id                                                  as event_id,
        event                                               as event_type,
        received_at                                         as event_ts,
        cast(order_id as {{ dbt_utils.type_string() }})     as event_details,
        cast(null as {{ dbt_utils.type_string() }})         as page_title,
        cast(context_page_path as {{ dbt_utils.type_string() }})                                    as page_url_path,
        cast(replace(
            {{ dbt_utils.get_url_host('context_page_referrer') }},
            'www.',
            ''
        )  as {{ dbt_utils.type_string() }})                as referrer_host,
        cast(null as {{ dbt_utils.type_string() }})         as search,
        cast(context_page_url as {{ dbt_utils.type_string() }})                                    as page_url,
        cast({{ dbt_utils.get_url_host('context_page_url') }} as {{ dbt_utils.type_string() }})    as page_url_host,
        cast({{ dbt_utils.get_url_parameter('context_page_url', 'gclid') }} as {{ dbt_utils.type_string() }}) as gclid,
        cast(null as {{ dbt_utils.type_string() }})         as utm_term,
        cast(null as {{ dbt_utils.type_string() }})         as utm_content,
        cast(null as {{ dbt_utils.type_string() }})         as utm_medium,
        cast(null as {{ dbt_utils.type_string() }})         as utm_campaign,
        cast(null as {{ dbt_utils.type_string() }})         as utm_source,
        cast(context_ip as {{ dbt_utils.type_string() }})                                          as ip,
        cast(anonymous_id as {{ dbt_utils.type_string() }})                                        as visitor_id,
        cast(user_id as {{ dbt_utils.type_string() }})                                             as user_id,
        case
            when lower(context_user_agent) like '%android%' then 'Android'
            else replace(
              split(context_user_agent,'(')[safe_offset(1)],
                ';', '')
        end as device,
        '{{ var('stg_segment_events_site') }}'  as site
    from source

)
,
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
