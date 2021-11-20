{% if var('product_warehouse_event_sources') %}

with events AS (SELECT * FROM {{ ref('int_web_events') }}
/*  {% if is_incremental() %}
    where visitor_id in (
        SELECT distinct visitor_id
        FROM {{ref('int_web_events')}}
        where CAST(event_ts AS datetime) >= (
          SELECT
            {{ dbt_utils.dateadd(
                'hour',
                -var('web_sessionization_trailing_window'),
                'max(event_ts)'
            ) }}
          FROM {{ this }})
        )
    {% endif %}
*/
),

numbered AS (

    SELECT

        *,

        row_number() over (
            PARTITION BYvisitor_id
            order by event_ts
          ) AS event_number

    FROM events

),

lagged AS (

    SELECT

        *,

        lag(event_ts) over (
            PARTITION BYvisitor_id
            order by event_number
          ) AS previous_event_ts

    FROM numbered

),

diffed AS (

    SELECT
        *,
        {{ dbt_utils.datediff('event_ts','previous_event_ts','second') }} AS period_of_inactivity

    FROM lagged

),

new_sessions AS (


    SELECT
        *,
        case
            when period_of_inactivity*-1 <= {{var('web_inactivity_cutoff')}} then 0
            else 1
        end AS new_session
    FROM diffed

),

session_numbers AS (


    SELECT

        *,

        sum(new_session) over (
            PARTITION BYvisitor_id
            order by event_number
            rows between unbounded preceding and current row
            ) AS session_number

    FROM new_sessions

),

session_ids AS (

  SELECT
    event_id,
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
    md5(CAST( CONCAT(coalesce(CAST(visitor_id AS string ),
     ''), '-', coalesce(CAST(session_number AS string ),
     '')) AS string )) AS session_id,
    site,
    order_id,
    total_revenue,
    currency_code
  FROM
    session_numbers ),
id_stitching AS (

    SELECT * FROM {{ref('int_web_events_user_stitching')}}

),

joined AS (

    SELECT

        session_ids.*,

        coalesce(id_stitching.user_id, session_ids.visitor_id)
            AS blended_user_id

    FROM session_ids
    left join id_stitching on id_stitching.visitor_id = session_ids.visitor_id

),
ordered AS (
  SELECT *,
         row_number() over (PARTITION BYblended_user_id order by event_ts) AS event_seq,
         row_number() over (PARTITION BYblended_user_id, session_id order by event_ts) AS event_in_session_seq
         ,

         case when event_type = 'Page View'
         and session_id = lead(session_id,1) over (PARTITION BYvisitor_id order by event_number)
         then {{ dbt_utils.datediff('lead(event_ts,1) over (PARTITION BYvisitor_id order by event_number)','event_ts','SECOND') }} end time_on_page_secs
  FROM joined

)
,
ordered_conversion_tagged AS (
  SELECT o.*
{% if var('attribution_conversion_event_type') %}
  ,
       case when o.event_type in ('{{ var('attribution_conversion_event_type') }}','{{ var('attribution_create_account_event_type') }}') then lag(o.page_url,1) over (PARTITION BYo.blended_user_id order by o.event_seq) end AS converting_page_url,
       case when o.event_type in ('{{ var('attribution_conversion_event_type') }}','{{ var('attribution_create_account_event_type') }}') then lag(o.page_title,1) over (PARTITION BYo.blended_user_id order by o.event_seq) end AS converting_page_title,
       case when o.event_type in ('{{ var('attribution_conversion_event_type') }}','{{ var('attribution_create_account_event_type') }}') then lag(o.page_url,2) over (PARTITION BYo.blended_user_id order by o.event_seq) end AS pre_converting_page_url,
       case when o.event_type in ('{{ var('attribution_conversion_event_type') }}','{{ var('attribution_create_account_event_type') }}') then lag(o.page_title,2) over (PARTITION BYo.blended_user_id order by o.event_seq) end AS pre_converting_page_title
{% endif %}
  FROM ordered o)
SELECT *
FROM ordered_conversion_tagged


{% else %}

  {{config(enabled=false)}}

{% endif %}
