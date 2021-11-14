{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("product_warehouse_event_sources") %}
{% if 'segment_events_page' in var("product_warehouse_event_sources") %}

with source as (

  select * from {{ source('segment', 'pages') }}

),

renamed as (

    select
        id                                                                   as event_id,
        'Page View'                                                          as event_type,
        received_at                                                          as event_ts,
        cast(context_page_title as {{ dbt_utils.type_string() }})            as event_details,
        cast(context_page_title as {{ dbt_utils.type_string() }})            as page_title,
        cast(path as {{ dbt_utils.type_string() }})                          as page_url_path,
        cast(replace(
            {{ dbt_utils.get_url_host('context_page_referrer') }},
            'www.',
            ''
        ) as {{ dbt_utils.type_string() }})                                 as referrer_host,
        cast(search as {{ dbt_utils.type_string() }})                       as search,
        cast(url as {{ dbt_utils.type_string() }})                          as page_url,
        cast({{ dbt_utils.get_url_host('url') }} as {{ dbt_utils.type_string() }})  as page_url_host,
        cast({{ dbt_utils.get_url_parameter('url', 'gclid') }} as {{ dbt_utils.type_string() }})  as gclid,
        cast(context_campaign_term as {{ dbt_utils.type_string() }})        as utm_term,
        cast(context_campaign_content as {{ dbt_utils.type_string() }})     as utm_content,
        cast(context_campaign_medium as {{ dbt_utils.type_string() }})      as utm_medium,
        cast(context_campaign_name as {{ dbt_utils.type_string() }})        as utm_campaign,
        cast(context_campaign_source as {{ dbt_utils.type_string() }})      as utm_source,
        cast(context_ip as {{ dbt_utils.type_string() }})                   as ip,
        cast(anonymous_id as {{ dbt_utils.type_string() }})                 as visitor_id,
        cast(user_id as {{ dbt_utils.type_string() }})                      as user_id,
        case
            when lower(context_user_agent) like '%android%' then 'Android'
            else replace(
              split(context_user_agent,'(')[safe_offset(1)],
                ';', '')
        end as device,
        cast('{{ var('stg_segment_events_site') }}' as {{ dbt_utils.type_string() }})  as site


    from source

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
