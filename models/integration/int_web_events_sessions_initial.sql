{% if var('product_warehouse_event_sources') %}

{{config(materialized="table")}}

{% set partition_by = "partition by session_id" %}

{% set window_clause = "
    partition by session_id
    order by event_number
    rows between unbounded preceding and unbounded following
    " %}

{% set first_values = {
    'utm_source' : 'utm_source',
    'utm_content' : 'utm_content',
    'utm_medium' : 'utm_medium',
    'utm_campaign' : 'utm_campaign',
    'utm_term' : 'utm_term',
    'search' : 'search',
    'gclid' : 'gclid',
    'page_url' : 'first_page_url',
    'page_url_host' : 'first_page_url_host',
    'page_url_path' : 'first_page_url_path',
    'referrer_host' : 'referrer_host',
    'device' : 'device',
    'device_category' : 'device_category'
    } %}

{% set last_values = {
    'page_url' : 'last_page_url',
    'page_url_host' : 'last_page_url_host',
    'page_url_path' : 'last_page_url_path',
    } %}

with events_sessionized as (

    select * from {{ref('int_web_events_sessionized')}}

    {% if is_incremental() %}
        where cast(event_ts as datetime) > (
          select
            {{ dbt_utils.dateadd(
                'hour',
                -var('web_sessionization_trailing_window'),
                'max(session_start_ts)'
            ) }}
          from {{ this }})
    {% endif %}

),

referrer_mapping as (

    select * from {{ ref('referrer_mapping') }}

),

additional_referrer_mapping as (

    select * from {{ ref('additional_referrer_mapping') }}

),
marketing_channel_mapping as (

    select * from {{ ref('marketing_channel_mapping') }}

),

channel_mapping as (

    select * from {{ ref('marketing_channel_mapping') }}

),
agg as (

    select distinct
        session_id,
        visitor_id,
        user_id,
        site,
        min(event_ts) over ( {{partition_by}} ) as session_start_ts,
        max(event_ts) over ( {{partition_by}} ) as session_end_ts,
        count(*) over ( {{partition_by}} ) as events,

        {% for (key, value) in first_values.items() %}
        first_value({{key}}) over ({{window_clause}}) as {{value}},
        {% endfor %}

        {% for (key, value) in last_values.items() %}
        last_value({{key}}) over ({{window_clause}}) as {{value}}{% if not loop.last %},{% endif %}
        {% endfor %}

    from events_sessionized

),

diffs as (

    select

        *,

        {{dbt_utils.datediff(
        'session_start_ts',
        'session_end_ts',
        'SECOND') }}

 as duration_in_s

    from agg

),

tiers as (

    select

        *,

        case
            when duration_in_s between 0 and 9 then '0s to 9s'
            when duration_in_s between 10 and 29 then '10s to 29s'
            when duration_in_s between 30 and 59 then '30s to 59s'
            when duration_in_s > 59 then '60s or more'
            else null
        end as duration_in_s_tier

    from diffs

),

mapped as (

    select
        tiers.*,
        referrer_mapping.medium as referrer_medium,
        referrer_mapping.source as referrer_source

    from tiers

    left join referrer_mapping on tiers.referrer_host = referrer_mapping.host

),

channel_mapped as (

    select
      mapped.*,
      case when coalesce(marketing_channel_mapping.channel,additional_referrer_mapping.channel) is not null then coalesce(marketing_channel_mapping.channel,additional_referrer_mapping.channel)
           when coalesce(marketing_channel_mapping.channel,additional_referrer_mapping.channel) is null and mapped.referrer_host is not null then 'Referral'
           else 'Direct' end as channel
      from mapped
      left join additional_referrer_mapping
      on mapped.referrer_host = additional_referrer_mapping.domain
      left join marketing_channel_mapping
      on mapped.utm_medium = marketing_channel_mapping.medium

)

select * from channel_mapped

{% else %}

  {{config(enabled=false)}}

{% endif %}
