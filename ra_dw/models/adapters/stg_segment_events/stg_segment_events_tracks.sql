{% if not var("enable_segment_events_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with source as (

    select * from {{ source('segment_events', 's_tracks') }}

),

renamed as (

    select
        id                          as track_id,
        event                       as event_name,
        event_text                  as event_text,
        cast(null as string )       as title,
        context_page_path           as context_page_path,
        context_page_referrer       as context_page_referrer,
        context_page_search         as context_page_search,
        context_page_url            as context_page_url,
        context_user_agent          as context_user_agent,
        context_campaign_term       as context_campaign_term,
        context_campaign_content    as context_campaign_content,
        context_campaign_medium     as context_campaign_medium,
        context_campaign_name       as context_campaign_name,
        context_campaign_source     as context_campaign_source,
        context_ip                  as context_ip,
        anonymous_id                as anonymous_id,
        user_id                     as user_id,
        received_at                 as track_ts
    from source

)

select * from renamed
