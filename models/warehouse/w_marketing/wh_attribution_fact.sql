{% if  var("marketing_warehouse_ad_campaign_sources") and var("product_warehouse_event_sources") %}
{{
    config(
      alias='attribution_fact'
    )
}}

WITH
converting_events as
    (
      SELECT
        e.blended_user_id,
        session_id,
        event_type,
        order_id,
        total_revenue,
        currency_code,
        1 as count_conversions,
        event_ts AS converted_ts
      FROM
        {{ref ('wh_web_events_fact') }} e
      WHERE
        event_type = '{{ var('attribution_conversion_event_type') }}'
  ),
converting_sessions_deduped as (
    SELECT
      blended_user_id AS blended_user_id,
      sum(total_revenue) as total_revenue,
      currency_code,
      sum(count_conversions) as count_conversions,
      session_id  session_id,
      MAX(converted_ts) AS converted_ts,
    FROM
      converting_events
    GROUP BY
     1,3,5
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
                (select c.count_conversions from converting_sessions_deduped c where c.session_id = s.session_id) as count_conversions,
                coalesce((select CASE WHEN (c.session_id = s.session_id)     THEN TRUE ELSE FALSE END  from converting_sessions_deduped c where c.session_id = s.session_id),false) AS conversion_session,
                coalesce((select CASE WHEN (c.session_id = s.session_id)     THEN 1 ELSE 0 END  from converting_sessions_deduped c where c.session_id = s.session_id),0) AS conversion_event,
                utm_source,
                utm_content,
                utm_medium,
                utm_campaign,
                referrer_host,
                first_page_url_host,
                split(net.reg_domain(referrer_host),'.')[OFFSET(0)] as referrer_domain,
                channel,
                events,
                (select c.total_revenue from converting_sessions_deduped c where c.session_id = s.session_id) as total_revenue,
                (select c.currency_code from converting_sessions_deduped c where c.session_id = s.session_id) as currency_code
              FROM
                {{ ref('wh_web_sessions_fact') }} s
            )

        ) WHERE
          conversion_cycle_conversion_ts >= session_start_ts
     )
          ,
  converting_sessions_deduped_labelled_with_conversion_number AS (
          SELECT
            *,
            SUM(conversion_event) over (PARTITION BY blended_user_id ORDER BY session_start_ts rows BETWEEN unbounded preceding AND CURRENT ROW)
            AS user_total_conversions,
          FROM
            converting_sessions_deduped_labelled
)
,
converting_sessions_deduped_labelled_with_conversion_cycles AS (
  SELECT *,
  CASE
    WHEN conversion_event = 0 THEN MAX(coalesce(user_total_conversions,0)) over (
      PARTITION BY blended_user_id
      ORDER BY
        session_start_ts rows BETWEEN unbounded preceding
        AND CURRENT ROW
    ) + 1
    ELSE MAX(user_total_conversions) over (
      PARTITION BY blended_user_id
      ORDER BY
        session_start_ts rows BETWEEN unbounded preceding
        AND CURRENT ROW
    )
  END AS user_conversion_cycle
  FROM converting_sessions_deduped_labelled_with_conversion_number
),
converting_sessions_deduped_labelled_with_session_day_number as (
  select
    *,
    {{ dbt_utils.datediff('"1900-01-01"','session_start_ts','day') }} as session_day_number
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
    if(is_within_attribution_time_decay_days_window,POW(2, days_before_conversion+1 / {{ var('attribution_time_decay_days_window') }}),null) AS time_decay_score,
    count(session_id) over (PARTITION BY blended_user_id,{{ dbt_utils.date_trunc('day','session_start_ts') }} ) as sessions_within_day_to_conversion
from
  days_to_each_conversion
),
split_time_decay_score_across_days_sessions as (
  select
    *,
    time_decay_score/sessions_within_day_to_conversion as apportioned_time_decay_score
  from
    add_time_decay_score
)
,
session_attrib_pct as (
    SELECT
      * except (first_page_url_host),
      if(conversion_session,0,CASE
        WHEN session_id = LAST_VALUE(if(is_within_attribution_lookback_window,session_id,null)  IGNORE NULLS) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)

        THEN 1
      ELSE
      0
    END)
      AS LAST_click_attrib_pct,
      if(conversion_session,0,CASE
        WHEN session_id = FIRST_VALUE(if(is_within_attribution_lookback_window,session_id,null) IGNORE NULLS) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        THEN 1
      ELSE
      0
    END)
      AS first_click_attrib_pct,
    if(conversion_session,0,IF(is_within_attribution_lookback_window,(1/  (COUNT(IF(is_within_attribution_lookback_window,session_id,null))
        OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)+0)-1),0)) AS even_click_attrib_pct,
    if(conversion_session,0,case when is_within_attribution_time_decay_days_window then
          apportioned_time_decay_score/(SUM(apportioned_time_decay_score) OVER(PARTITION BY blended_user_id, user_conversion_cycle)) end) AS time_decay_attrib_pct
from split_time_decay_score_across_days_sessions
),
final as (
    SELECT
      * ,
      (MAX(count_conversions) over (partition by blended_user_id, user_conversion_cycle) * first_click_attrib_pct) AS first_click_attrib_conversions,
      (MAX(count_conversions) over (partition by blended_user_id, user_conversion_cycle) * last_click_attrib_pct) AS last_click_attrib_conversions,
      (MAX(count_conversions) over (partition by blended_user_id, user_conversion_cycle) * even_click_attrib_pct) AS even_click_attrib_conversions,
      (MAX(count_conversions) over (partition by blended_user_id, user_conversion_cycle) * time_decay_attrib_pct) AS time_decay_attrib_conversions,
      (MAX(total_revenue) over (partition by blended_user_id, user_conversion_cycle) * first_click_attrib_pct) AS first_click_attrib_revenue,
      (MAX(total_revenue) over (partition by blended_user_id, user_conversion_cycle) * last_click_attrib_pct) AS last_click_attrib_revenue,
      (MAX(total_revenue) over (partition by blended_user_id, user_conversion_cycle) * even_click_attrib_pct) AS even_click_attrib_revenue,
      (MAX(total_revenue) over (partition by blended_user_id, user_conversion_cycle) * time_decay_attrib_pct) AS time_decay_attrib_revenue
    FROM
      session_attrib_pct

    {{ dbt_utils.group_by(33) }}
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
  total_revenue,
  currency_code,
  user_conversion_cycle,
  days_before_conversion,
  is_within_attribution_lookback_window,
  is_within_attribution_time_decay_days_window,
  sessions_within_day_to_conversion,
  first_click_attrib_pct,
  last_click_attrib_pct,
  even_click_attrib_pct,
  time_decay_attrib_pct,
  first_click_attrib_conversions,
  last_click_attrib_conversions,
  even_click_attrib_conversions,
  time_decay_attrib_conversions,
  first_click_attrib_revenue,
  last_click_attrib_revenue,
  even_click_attrib_revenue,
  time_decay_attrib_revenue
from
  final

{% endif %}
