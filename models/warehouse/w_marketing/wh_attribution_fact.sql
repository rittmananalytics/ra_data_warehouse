{% if  var("marketing_warehouse_ad_campaign_sources") and var("product_warehouse_event_sources") %}
{{
    config(
      alias='attribution_fact'
    )
}}


WITH
events_filtered as (
  SELECT
    *
  FROM (
    SELECT
      *,
      FIRST_VALUE(CASE WHEN event_type = '{{ var('attribution_create_account_event_type') }}' THEN event_id END) OVER (PARTITION BY blended_user_id order by event_ts ROWS BETWEEN unbounded preceding AND unbounded following) AS first_registration_event_id,
      FIRST_VALUE(CASE WHEN event_type = '{{ var('attribution_conversion_event_type') }}' THEN event_id END) OVER (PARTITION BY blended_user_id order by event_ts ROWS BETWEEN unbounded preceding AND unbounded following) AS first_order_event_id
    FROM
      {{ ref ('wh_web_events_fact') }})
  WHERE
    event_type != '{{ var('attribution_create_account_event_type') }}'
    OR (event_id = first_registration_event_id)
),
converting_events as
    (
      SELECT
        e.blended_user_id,
        session_id,
        event_type,
        order_id,
        case when event_type = '{{ var('attribution_conversion_event_type') }}' and event_id = first_order_event_id then total_revenue else 0 end as first_order_total_revenue,
        case when event_type = '{{ var('attribution_conversion_event_type') }}' and event_id != first_order_event_id then total_revenue else 0 end as repeat_order_total_revenue,
        currency_code,
        case when event_type in ('{{ var('attribution_conversion_event_type') }}','{{ var('attribution_create_account_event_type') }}') then 1 else 0 end as count_conversions,
        case when event_type = '{{ var('attribution_conversion_event_type') }}' and event_id = first_order_event_id then 1 else 0 end as count_first_order_conversions,
        case when event_type = '{{ var('attribution_conversion_event_type') }}' and event_id != first_order_event_id then 1 else 0 end as count_repeat_order_conversions,
        case when event_type = '{{ var('attribution_conversion_event_type') }}' then 1 else 0 end as count_order_conversions,
        case when event_type = '{{ var('attribution_create_account_event_type') }}' then 1 else 0 end as count_registration_conversions,
        event_ts AS converted_ts
      FROM
       events_filtered e
      WHERE
        event_type in ('{{ var('attribution_conversion_event_type') }}','{{ var('attribution_create_account_event_type')}}')
  ),
converting_sessions_deduped as (
    SELECT
      session_id  session_id,
      max(blended_user_id) AS blended_user_id,
      sum(first_order_total_revenue) as first_order_total_revenue,
      sum(repeat_order_total_revenue) as repeat_order_total_revenue,
      max(currency_code) as currency_code,
      sum(count_first_order_conversions) as count_first_order_conversions,
      sum(count_repeat_order_conversions) as count_repeat_order_conversions,
      sum(count_order_conversions) as count_order_conversions,
      sum(count_registration_conversions) as count_registration_conversions,
      sum(count_registration_conversions)+sum(count_first_order_conversions)+sum(count_repeat_order_conversions) as count_conversions,
      MAX(converted_ts) AS converted_ts
    FROM
      converting_events
    GROUP BY
     1
  ),
  converting_sessions_deduped_labelled as
      (
        SELECT
          *
          FROM (
            SELECT
              *,
              FIRST_VALUE(converted_ts ignore nulls) over (PARTITION BY blended_user_id ORDER BY session_start_ts rows BETWEEN current row AND unbounded following) as conversion_cycle_conversion_ts
            FROM (
              SELECT
                s.blended_user_id,
                s.session_start_ts,
                s.session_end_ts,
                (select c.converted_ts from converting_sessions_deduped c where c.session_id = s.session_id) as converted_ts,
                s.session_id AS session_id,
                ROW_NUMBER() OVER (PARTITION BY s.blended_user_id ORDER BY s.session_start_ts) AS session_seq,
                (select max(c.count_conversions) from converting_sessions_deduped c where c.session_id = s.session_id) as count_conversions,
                (select max(c.count_order_conversions) from converting_sessions_deduped c where c.session_id = s.session_id) as count_order_conversions,
                (select max(c.count_first_order_conversions) from converting_sessions_deduped c where c.session_id = s.session_id) as count_first_order_conversions,
                (select max(c.count_repeat_order_conversions) from converting_sessions_deduped c where c.session_id = s.session_id) as count_repeat_order_conversions,
                (select max(c.count_registration_conversions) from converting_sessions_deduped c where c.session_id = s.session_id) as count_registration_conversions,
                coalesce((select CASE WHEN (c.session_id = s.session_id) THEN TRUE ELSE FALSE END  from converting_sessions_deduped c where c.session_id = s.session_id),false) AS conversion_session,
                coalesce((select CASE WHEN (c.session_id = s.session_id) THEN 1 ELSE 0 END  from converting_sessions_deduped c where c.session_id = s.session_id),0) AS conversion_event,
                coalesce((select CASE WHEN (c.session_id = s.session_id and c.count_order_conversions>1) THEN 1 ELSE 0 END  from converting_sessions_deduped c where c.session_id = s.session_id),0) AS order_conversion_event,
                coalesce((select CASE WHEN (c.session_id = s.session_id and c.count_registration_conversions>1) THEN 1 ELSE 0 END  from converting_sessions_deduped c where c.session_id = s.session_id),0) AS registration_conversion_event,
                coalesce((select CASE WHEN (c.session_id = s.session_id and c.count_first_order_conversions>1) THEN 1 ELSE 0 END  from converting_sessions_deduped c where c.session_id = s.session_id),0) AS first_order_conversion_event,
                coalesce((select CASE WHEN (c.session_id = s.session_id and c.count_repeat_order_conversions>1) THEN 1 ELSE 0 END  from converting_sessions_deduped c where c.session_id = s.session_id),0) AS repeat_order_conversion_event,
                utm_source,
                utm_content,
                utm_medium,
                utm_campaign,
                referrer_host,
                first_page_url_host,
                split(net.reg_domain(referrer_host),'.')[OFFSET(0)] as referrer_domain,
                channel,
                case when lower(channel) = 'direct' then false else true end as is_non_direct_channel,
                case when lower(channel) like '%paid%' then true else false end as is_paid_channel,
                events,
                (select c.first_order_total_revenue from converting_sessions_deduped c where c.session_id = s.session_id) as first_order_total_revenue,
                (select c.repeat_order_total_revenue from converting_sessions_deduped c where c.session_id = s.session_id) as repeat_order_total_revenue,
                (select c.currency_code from converting_sessions_deduped c where c.session_id = s.session_id) as currency_code
              FROM
                {{ ref('wh_web_sessions_fact') }} s
            )
        )   WHERE
          conversion_cycle_conversion_ts >= session_start_ts
)
          ,
  converting_sessions_deduped_labelled_with_conversion_number AS (
        SELECT
          *,
          SUM(conversion_event) over (PARTITION BY blended_user_id ORDER BY session_start_ts rows BETWEEN unbounded preceding AND CURRENT ROW) AS user_total_conversions,
          SUM(count_order_conversions) over (PARTITION BY blended_user_id ORDER BY session_start_ts rows BETWEEN unbounded preceding AND CURRENT ROW) AS user_total_order_conversions,
          SUM(count_registration_conversions) over (PARTITION BY blended_user_id ORDER BY session_start_ts rows BETWEEN unbounded preceding AND CURRENT ROW) AS user_total_registration_conversions,
          SUM(count_first_order_conversions) over (PARTITION BY blended_user_id ORDER BY session_start_ts rows BETWEEN unbounded preceding AND CURRENT ROW) AS user_total_first_order_conversions,
          SUM(count_repeat_order_conversions) over (PARTITION BY blended_user_id ORDER BY session_start_ts rows BETWEEN unbounded preceding AND CURRENT ROW) AS user_total_repeat_order_conversions
        FROM
            converting_sessions_deduped_labelled
)
,
converting_sessions_deduped_labelled_with_conversion_cycles AS (
    SELECT
      * ,
      CASE WHEN registration_conversion_event = 0   THEN MAX(coalesce(user_total_registration_conversions,0)) over (PARTITION BY blended_user_id
        ORDER BY session_start_ts rows BETWEEN unbounded preceding AND CURRENT ROW) + 1
      ELSE MAX(user_total_registration_conversions) over (PARTITION BY blended_user_id
        ORDER BY session_start_ts rows BETWEEN unbounded preceding AND CURRENT ROW)
      END AS user_registration_conversion_cycle,

      CASE WHEN conversion_event = 0                THEN MAX(coalesce(user_total_conversions,0))              over (PARTITION BY blended_user_id
        ORDER BY session_start_ts ROWS BETWEEN unbounded preceding AND CURRENT ROW) + 1
      ELSE MAX(user_total_conversions) over (PARTITION BY blended_user_id
        ORDER BY session_start_ts ROWS BETWEEN unbounded preceding AND CURRENT ROW)
      END AS user_conversion_cycle,

      CASE WHEN first_order_conversion_event = 0 THEN MAX(coalesce(user_total_first_order_conversions,0))     over (PARTITION BY blended_user_id
          ORDER BY session_start_ts rows BETWEEN unbounded preceding AND CURRENT ROW) + 1
        ELSE MAX(user_total_first_order_conversions) over (PARTITION BY blended_user_id
          ORDER BY session_start_ts rows BETWEEN unbounded preceding AND CURRENT ROW)
      END as user_first_order_conversion_cycle,

      CASE WHEN repeat_order_conversion_event = 0 THEN MAX(coalesce(user_total_repeat_order_conversions,0)) over (PARTITION BY blended_user_id
            ORDER BY session_start_ts rows BETWEEN unbounded preceding AND CURRENT ROW) + 1
          ELSE MAX(user_total_repeat_order_conversions) over (PARTITION BY blended_user_id
            ORDER BY session_start_ts rows BETWEEN unbounded preceding AND CURRENT ROW)
          END as user_repeat_order_conversion_cycle
    FROM converting_sessions_deduped_labelled_with_conversion_number
),
converting_sessions_deduped_labelled_with_session_day_number as (
  select
    *,

    {{ dbt_utils.datediff('"1900-01-01"','session_start_ts','day') }}
 as session_day_number
  from
    converting_sessions_deduped_labelled_with_conversion_cycles
),
days_to_each_conversion as (
  select
    *,
    session_day_number - max(session_day_number) over (partition by blended_user_id, user_conversion_cycle)  as days_before_conversion,
    (session_day_number - max(session_day_number) over (partition by blended_user_id, user_conversion_cycle))*-1 <= {{ var('attribution_lookback_days_window') }} as is_within_attribution_lookback_window,
    (session_day_number - max(session_day_number) over (partition by blended_user_id, user_conversion_cycle))*-1 <= {{ var('attribution_time_decay_days_window') }} as is_within_attribution_time_decay_days_window
  from
    converting_sessions_deduped_labelled_with_session_day_number
),
add_time_decay_score as (
  select
    *,
    if(is_within_attribution_time_decay_days_window,safe_divide
        (POW(2, (days_before_conversion-1)) ,( {{ var('attribution_time_decay_days_window') }}))
      ,null) AS time_decay_score,
    if(conversion_session AND NOT {{ var('attribution_include_conversion_session') }},0,POW(2, (days_before_conversion-1) )) as weighting,
    if(conversion_session AND NOT {{ var('attribution_include_conversion_session') }},0,
        (count
          (case when not conversion_session or {{ var('attribution_include_conversion_session') }} then session_id end) over (PARTITION BY blended_user_id,{{ dbt_utils.date_trunc('day','session_start_ts') }}
        )
      )
    ) as sessions_within_day_to_conversion,
    if(conversion_session AND NOT {{ var('attribution_include_conversion_session') }},0,safe_divide
      (POW(2, (days_before_conversion-1)),count
        (case when not conversion_session or {{ var('attribution_include_conversion_session') }}  then session_id end) over (PARTITION BY blended_user_id,{{ dbt_utils.date_trunc('day','session_start_ts') }})
      )
    ) as weighting_split_by_days_sessions
  from
    days_to_each_conversion
),
split_time_decay_score_across_days_sessions as (
  select
    *,
    safe_divide(time_decay_score,sessions_within_day_to_conversion) as apportioned_time_decay_score
  from
    add_time_decay_score
)
,
session_attrib_pct as (
    SELECT
      * except (first_page_url_host),
      if(conversion_session and not {{ var('attribution_include_conversion_session') }},0,CASE
        WHEN session_id = LAST_VALUE(if(is_within_attribution_lookback_window and (not conversion_session or {{ var('attribution_include_conversion_session') }}),session_id,null)  IGNORE NULLS) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)

        THEN 1
      ELSE
      0
    END)
      AS last_click_attrib_pct,
      if(conversion_session and not {{ var('attribution_include_conversion_session') }},0,CASE
        WHEN session_id = LAST_VALUE(if(is_within_attribution_lookback_window and (not conversion_session or {{ var('attribution_include_conversion_session') }}) and is_non_direct_channel,session_id,null)  IGNORE NULLS) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)

        THEN 1
      ELSE
      0
    END)
      AS last_non_direct_click_attrib_pct,
      if(conversion_session and not {{ var('attribution_include_conversion_session') }},0,CASE
        WHEN session_id = LAST_VALUE(if(is_within_attribution_lookback_window and (not conversion_session or {{ var('attribution_include_conversion_session') }}) and is_paid_channel,session_id,null)  IGNORE NULLS) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)

        THEN 1
      ELSE
      0
    END)
      AS last_paid_click_attrib_pct,
      if(conversion_session and not {{ var('attribution_include_conversion_session') }},0,CASE
        WHEN session_id = FIRST_VALUE(if(is_within_attribution_lookback_window,session_id,null) IGNORE NULLS) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        THEN 1
      ELSE
      0
    END)
      AS first_click_attrib_pct,
    if(conversion_session and not {{ var('attribution_include_conversion_session') }},0,CASE
        WHEN session_id = FIRST_VALUE(if(is_within_attribution_lookback_window and (not conversion_session or {{ var('attribution_include_conversion_session') }}) and is_non_direct_channel,session_id,null) IGNORE NULLS) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        THEN 1
      ELSE
      0
    END)
      AS first_non_direct_click_attrib_pct,
    if(conversion_session and not {{ var('attribution_include_conversion_session') }},0,CASE
          WHEN session_id = FIRST_VALUE(if(is_within_attribution_lookback_window  and (not conversion_session or {{ var('attribution_include_conversion_session') }}) and is_paid_channel,session_id,null) IGNORE NULLS) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
          THEN 1
        ELSE
        0
      END)
        AS first_paid_click_attrib_pct,
    if(conversion_session and not {{ var('attribution_include_conversion_session') }},0,
      IF(is_within_attribution_lookback_window,
          (safe_divide
            (1,
              (COUNT
                (IF
                  (is_within_attribution_lookback_window,session_id,null)
                )
                OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING){% if  var('attribution_include_conversion_session')  %} +0 {% else %} -1 {% endif %}
              )
            )
          ),0
        )
      ) AS even_click_attrib_pct,
    IF(conversion_session and not {{ var('attribution_include_conversion_session') }},0,
      case when is_within_attribution_time_decay_days_window then safe_divide(apportioned_time_decay_score,(SUM(apportioned_time_decay_score) OVER(PARTITION BY blended_user_id, user_conversion_cycle)))
        end
      ) AS time_decay_attrib_pct
  FROM
    split_time_decay_score_across_days_sessions
),
final as (
    SELECT
      * ,
      (MAX(count_registration_conversions) over (partition by blended_user_id, user_conversion_cycle) * first_click_attrib_pct) AS user_registration_first_click_attrib_conversions,
      (MAX(count_registration_conversions) over (partition by blended_user_id, user_conversion_cycle) * first_non_direct_click_attrib_pct) AS user_registration_first_non_direct_click_attrib_conversions,
      (MAX(count_registration_conversions) over (partition by blended_user_id, user_conversion_cycle) * first_paid_click_attrib_pct) AS user_registration_first_paid_click_attrib_conversions,
      (MAX(count_registration_conversions) over (partition by blended_user_id, user_conversion_cycle) * last_click_attrib_pct) AS user_registration_last_click_attrib_conversions,
      (MAX(count_registration_conversions) over (partition by blended_user_id, user_conversion_cycle) * last_non_direct_click_attrib_pct) AS user_registration_last_non_direct_click_attrib_conversions,
      (MAX(count_registration_conversions) over (partition by blended_user_id, user_conversion_cycle) * last_paid_click_attrib_pct) AS user_registration_last_paid_click_attrib_conversions,
      (MAX(count_registration_conversions) over (partition by blended_user_id, user_conversion_cycle) * even_click_attrib_pct) AS user_registration_even_click_attrib_conversions,
      (MAX(count_registration_conversions) over (partition by blended_user_id, user_conversion_cycle) * time_decay_attrib_pct) AS user_registration_time_decay_attrib_conversions,
      (MAX(count_first_order_conversions) over (partition by blended_user_id, user_conversion_cycle) * first_click_attrib_pct) AS first_order_first_click_attrib_conversions,
      (MAX(count_first_order_conversions) over (partition by blended_user_id, user_conversion_cycle) * first_non_direct_click_attrib_pct) AS first_order_first_non_direct_click_attrib_conversions,
      (MAX(count_first_order_conversions) over (partition by blended_user_id, user_conversion_cycle) * first_paid_click_attrib_pct) AS first_order_first_paid_click_attrib_conversions,
      (MAX(count_first_order_conversions) over (partition by blended_user_id, user_conversion_cycle) * last_click_attrib_pct) AS first_order_last_click_attrib_conversions,
      (MAX(count_first_order_conversions) over (partition by blended_user_id, user_conversion_cycle) * last_non_direct_click_attrib_pct) AS first_order_last_non_direct_click_attrib_conversions,
      (MAX(count_first_order_conversions) over (partition by blended_user_id, user_conversion_cycle) * last_paid_click_attrib_pct) AS first_order_last_paid_click_attrib_conversions,
      (MAX(count_first_order_conversions) over (partition by blended_user_id, user_conversion_cycle) * even_click_attrib_pct) AS first_order_even_click_attrib_conversions,
      (MAX(count_first_order_conversions) over (partition by blended_user_id, user_conversion_cycle) * time_decay_attrib_pct) AS first_order_time_decay_attrib_conversions,
      (MAX(first_order_total_revenue) over (partition by blended_user_id, user_conversion_cycle) * first_click_attrib_pct) AS first_order_first_click_attrib_revenue,
      (MAX(first_order_total_revenue) over (partition by blended_user_id, user_conversion_cycle) * first_non_direct_click_attrib_pct) AS first_order_first_non_direct_click_attrib_revenue,
      (MAX(first_order_total_revenue) over (partition by blended_user_id, user_conversion_cycle) * first_paid_click_attrib_pct) AS first_order_first_paid_click_attrib_revenue,
      (MAX(first_order_total_revenue) over (partition by blended_user_id, user_conversion_cycle) * last_click_attrib_pct) AS first_order_last_click_attrib_revenue,
      (MAX(first_order_total_revenue) over (partition by blended_user_id, user_conversion_cycle) * last_non_direct_click_attrib_pct) AS first_order_last_non_direct_click_attrib_revenue,
      (MAX(first_order_total_revenue) over (partition by blended_user_id, user_conversion_cycle) * last_paid_click_attrib_pct) AS first_order_last_paid_click_attrib_revenue,
      (MAX(first_order_total_revenue) over (partition by blended_user_id, user_conversion_cycle) * even_click_attrib_pct) AS first_order_even_click_attrib_revenue,
      (MAX(first_order_total_revenue) over (partition by blended_user_id, user_conversion_cycle) * time_decay_attrib_pct) AS first_order_time_decay_attrib_revenue,
      (MAX(count_repeat_order_conversions) over (partition by blended_user_id, user_conversion_cycle) * first_click_attrib_pct) AS repeat_order_first_click_attrib_conversions,
      (MAX(count_repeat_order_conversions) over (partition by blended_user_id, user_conversion_cycle) * first_non_direct_click_attrib_pct) AS repeat_order_first_non_direct_click_attrib_conversions,
      (MAX(count_repeat_order_conversions) over (partition by blended_user_id, user_conversion_cycle) * first_paid_click_attrib_pct) AS repeat_order_first_paid_click_attrib_conversions,
      (MAX(count_repeat_order_conversions) over (partition by blended_user_id, user_conversion_cycle) * last_click_attrib_pct) AS repeat_order_last_click_attrib_conversions,
      (MAX(count_repeat_order_conversions) over (partition by blended_user_id, user_conversion_cycle) * last_non_direct_click_attrib_pct) AS repeat_order_last_non_direct_click_attrib_conversions,
      (MAX(count_repeat_order_conversions) over (partition by blended_user_id, user_conversion_cycle) * last_paid_click_attrib_pct) AS repeat_order_last_paid_click_attrib_conversions,
      (MAX(count_repeat_order_conversions) over (partition by blended_user_id, user_conversion_cycle) * even_click_attrib_pct) AS repeat_order_even_click_attrib_conversions,
      (MAX(count_repeat_order_conversions) over (partition by blended_user_id, user_conversion_cycle) * time_decay_attrib_pct) AS repeat_order_time_decay_attrib_conversions,
      (MAX(repeat_order_total_revenue) over (partition by blended_user_id, user_conversion_cycle) * first_click_attrib_pct) AS repeat_order_first_click_attrib_revenue,
      (MAX(repeat_order_total_revenue) over (partition by blended_user_id, user_conversion_cycle) * first_non_direct_click_attrib_pct) AS repeat_order_first_non_direct_click_attrib_revenue,
      (MAX(repeat_order_total_revenue) over (partition by blended_user_id, user_conversion_cycle) * first_paid_click_attrib_pct) AS repeat_order_first_paid_click_attrib_revenue,
      (MAX(repeat_order_total_revenue) over (partition by blended_user_id, user_conversion_cycle) * last_click_attrib_pct) AS repeat_order_last_click_attrib_revenue,
      (MAX(repeat_order_total_revenue) over (partition by blended_user_id, user_conversion_cycle) * last_non_direct_click_attrib_pct) AS repeat_order_last_non_direct_click_attrib_revenue,
      (MAX(repeat_order_total_revenue) over (partition by blended_user_id, user_conversion_cycle) * last_paid_click_attrib_pct) AS repeat_order_last_paid_click_attrib_revenue,
      (MAX(repeat_order_total_revenue) over (partition by blended_user_id, user_conversion_cycle) * even_click_attrib_pct) AS repeat_order_even_click_attrib_revenue,
      (MAX(repeat_order_total_revenue) over (partition by blended_user_id, user_conversion_cycle) * time_decay_attrib_pct) AS repeat_order_time_decay_attrib_revenue
    FROM
      session_attrib_pct

   {{ dbt_utils.group_by(57) }}
)
select
  blended_user_id,
  session_start_ts,
  session_end_ts,
  session_id,
  session_seq,
  conversion_session,
  utm_source,
  utm_content,
  utm_medium,
  utm_campaign,
  referrer_host,
  referrer_domain,
  channel,
  first_order_total_revenue,
  repeat_order_total_revenue,
  currency_code,
  user_conversion_cycle,
  user_registration_conversion_cycle,
  user_first_order_conversion_cycle,
  user_repeat_order_conversion_cycle,
  is_within_attribution_lookback_window,
  is_within_attribution_time_decay_days_window,
  is_non_direct_channel,
  is_paid_channel,
  sessions_within_day_to_conversion,
  time_decay_score,
  apportioned_time_decay_score,
  days_before_conversion,
  weighting as time_decay_score_weighting,
  weighting_split_by_days_sessions as time_decay_weighting_split_by_days_sessions,
  count_conversions,
  count_order_conversions,
  count_first_order_conversions,
  count_repeat_order_conversions,
  count_registration_conversions,
  first_click_attrib_pct,
  first_non_direct_click_attrib_pct,
  first_paid_click_attrib_pct,
  last_click_attrib_pct,
  last_non_direct_click_attrib_pct,
  last_paid_click_attrib_pct,
  even_click_attrib_pct,
  time_decay_attrib_pct,
  user_registration_first_click_attrib_conversions,
  user_registration_first_non_direct_click_attrib_conversions,
  user_registration_first_paid_click_attrib_conversions,
  user_registration_last_click_attrib_conversions,
  user_registration_last_non_direct_click_attrib_conversions,
  user_registration_last_paid_click_attrib_conversions,
  user_registration_even_click_attrib_conversions,
  user_registration_time_decay_attrib_conversions,
  first_order_first_click_attrib_conversions,
  first_order_first_non_direct_click_attrib_conversions,
  first_order_first_paid_click_attrib_conversions,
  first_order_last_click_attrib_conversions,
  first_order_last_non_direct_click_attrib_conversions,
  first_order_last_paid_click_attrib_conversions,
  first_order_even_click_attrib_conversions,
  first_order_time_decay_attrib_conversions,
  first_order_first_click_attrib_revenue,
  first_order_first_non_direct_click_attrib_revenue,
  first_order_first_paid_click_attrib_revenue,
  first_order_last_click_attrib_revenue,
  first_order_last_non_direct_click_attrib_revenue,
  first_order_last_paid_click_attrib_revenue,
  first_order_even_click_attrib_revenue,
  first_order_time_decay_attrib_revenue,
  repeat_order_first_click_attrib_conversions,
  repeat_order_first_non_direct_click_attrib_conversions,
  repeat_order_first_paid_click_attrib_conversions,
  repeat_order_last_click_attrib_conversions,
  repeat_order_last_non_direct_click_attrib_conversions,
  repeat_order_last_paid_click_attrib_conversions,
  repeat_order_even_click_attrib_conversions,
  repeat_order_time_decay_attrib_conversions,
  repeat_order_first_click_attrib_revenue,
  repeat_order_first_non_direct_click_attrib_revenue,
  repeat_order_first_paid_click_attrib_revenue,
  repeat_order_last_click_attrib_revenue,
  repeat_order_last_non_direct_click_attrib_revenue,
  repeat_order_last_paid_click_attrib_revenue,
  repeat_order_even_click_attrib_revenue,
  repeat_order_time_decay_attrib_revenue
from
  final
{% endif %}
