{% if not var("enable_segment_events_source") and not var("enable_mixpanel_events_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

{#
the initial CTE in this model is unusually complicated; its function is to
select all events (for all time) for users who have pageviews since the
model was most recently run. there are many window functions in this model so
in order to appropriately calculate all of them we need each user's entire
event history, but we only want to grab that for users who have events we need to calculate.
#}

with events as (

    select * from {{ref('int_web_events')}}

    {% if is_incremental() %}
    where visitor_id in (
        select distinct visitor_id
        from {{ref('int_web_events')}}
        where cast(event_ts as datetime) >= (
          select
            {{ dbt_utils.dateadd(
                'hour',
                -var('web_sessionization_trailing_window'),
                'max(event_ts)'
            ) }}
          from {{ this }})
        )
    {% endif %}

),

numbered as (

    --This CTE is responsible for assigning an all-time event number for a
    --given visitor_id. We don't need to do this across devices because the
    --whole point of this field is for sessionization, and sessions can't span
    --multiple devices.

    select

        *,

        row_number() over (
            partition by visitor_id
            order by event_ts
          ) as event_number

    from events

),

lagged as (

    --This CTE is responsible for simply grabbing the last value of `event_ts`.
    --We'll use this downstream to do timestamp math--it's how we determine the
    --period of inactivity.

    select

        *,

        lag(event_ts) over (
            partition by visitor_id
            order by event_number
          ) as previous_event_ts

    from numbered

),

diffed as (

    --This CTE simply calculates `period_of_inactivity`.

    select
        *,
        timestamp_diff(event_ts,previous_event_ts,second) as period_of_inactivity

    from lagged

),

new_sessions as (

    --This CTE calculates a single 1/0 field--if the period of inactivity prior
    --to this page view was greater than 30 minutes, the value is 1, otherwise
    --it's 0. We'll use this to calculate the user's session #.

    select
        *,
        case
            when period_of_inactivity <= {{var('web_inactivity_cutoff')}} then 0
            else 1
        end as new_session
    from diffed

),

session_numbers as (

    --This CTE calculates a user's session (1, 2, 3) number from `new_session`.
    --This single field is the entire point of the entire prior series of
    --calculations.

    select

        *,

        sum(new_session) over (
            partition by visitor_id
            order by event_number
            rows between unbounded preceding and current row
            ) as session_number

    from new_sessions

),

session_ids AS (
    --This CTE assigns a globally unique session id based on the combination of
    --`anonymous_id` and `session_number`.
  SELECT
    event_type,
    event_ts,
    event_details,
    page_title,
    page_url_path,
    referrer_host,
    search,
    page_url,
    page_url_host,
    gclid,
    utm_term,
    utm_content,
    utm_medium,
    utm_campaign,
    utm_source,
    ip,
    visitor_id,
    user_id,
    device,
    device_category,
    event_number,
    to_hex(md5(CAST( CONCAT(coalesce(CAST(visitor_id AS string ),
              ''), '-', coalesce(CAST(session_number AS string ),
              '')) AS string ))) AS session_id
  FROM
    session_numbers ),
id_stitching as (

    select * from {{ref('int_web_events_user_stitching')}}

),

joined as (

    select

        session_ids.*,

        coalesce(id_stitching.user_id, session_ids.visitor_id)
            as blended_user_id

    from session_ids
    left join id_stitching on id_stitching.visitor_id = session_ids.visitor_id

),
ordered as (
  select *,
         row_number() over (partition by blended_user_id order by event_ts) as event_seq,
         row_number() over (partition by blended_user_id, session_id order by event_ts) as event_in_session_seq,

         case when event_type = 'Page View' then timestamp_diff(lead(event_ts) over (partition by visitor_id order by event_number),event_ts,SECOND) end time_on_page_secs
  from joined

),
geo_located as (
  SELECT
  a.* except(clientIpNum,classB),
  IFNULL(city, 'Other') AS city,
  IFNULL(countryLabel, 'Other') AS countryLabel,
  latitude,
  longitude
FROM (
  SELECT
    *,
    CASE WHEN BYTE_LENGTH(ip) < 16 THEN SAFE_CAST(NET.IPV4_TO_INT64(NET.SAFE_IP_FROM_STRING(ip)) AS INT64) ELSE NULL END AS clientIpNum,
    CASE WHEN BYTE_LENGTH(ip) < 16 THEN SAFE_CAST(NET.IPV4_TO_INT64(NET.SAFE_IP_FROM_STRING(ip)) / (256*256) AS INT64) ELSE NULL END AS classB
  FROM
    ordered ) AS a
LEFT OUTER JOIN
  `ra-development.company_website.geolite_city` AS b
ON
  a.classB = b.classB
  AND a.clientIpNum BETWEEN b.startIpNum AND b.endIpNum
)
select * from geo_located
