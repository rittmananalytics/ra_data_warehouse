{% if not enable_segment_events %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with source as (

    select * from {{ source('segment_warehouse', 'tracks') }}

),

renamed as (

    select
        anonymous_id,
        context_ip,
        context_library_name,
        context_library_version,
        context_page_path,
        context_page_referrer,
        context_page_title,
        context_page_url,
        context_user_agent,
        event,
        event_text,
        id,
        loaded_at,
        original_timestamp,
        received_at,
        sent_at,
        timestamp,
        uuid_ts,
        context_page_search,
        user_id,
        context_campaign_medium,
        context_campaign_name,
        context_campaign_source,
        context_campaign_term,
        context_campaign_content,
        context_protocols_source_id,
        context_locale

    from source

)

select * from renamed
