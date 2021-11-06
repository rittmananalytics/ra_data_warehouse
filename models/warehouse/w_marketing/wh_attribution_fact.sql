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
        first_value(CASE WHEN event_type = '{{ var('attribution_conversion_event_type') }}' THEN session_id END) over (PARTITION BY e.blended_user_id order by e.event_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as session_id,
        event_type,
        order_id,
        total_revenue,
        currency_code,
        1 as count_conversions,
        MIN(CASE WHEN event_type = '{{ var('attribution_conversion_event_type') }}' THEN event_ts END ) OVER (PARTITION BY e.blended_user_id) AS converted_ts,
        MIN(CASE WHEN event_type = '{{ var('attribution_create_account_event_type') }}' THEN event_ts END ) OVER (PARTITION BY e.blended_user_id) AS created_account_ts
      FROM
        {{ref ('wh_web_events_fact') }} e
      WHERE
        event_type = '{{ var('attribution_conversion_event_type') }}'
        OR event_type = '{{ var('attribution_create_account_event_type') }}'),
converting_sessions as (
    SELECT
      *
    FROM
      converting_events
    {{ dbt_utils.group_by(9) }}
  ),
converting_sessions_deduped as (
    SELECT
      blended_user_id AS blended_user_id,
      order_id,
      total_revenue,
      currency_code,
      max(count_conversions) as count_conversions,
      MAX(CASE WHEN event_type = '{{ var('attribution_conversion_event_type') }}' THEN session_id END ) AS session_id,
      MIN(converted_ts) AS converted_ts,
      MIN(created_account_ts) AS created_account_ts
    FROM
      converting_sessions
    GROUP BY
     1,2,3,4
  ),
converting_sessions_deduped_labelled as
    (
      SELECT
        c.blended_user_id,
        s.session_start_ts,
        s.session_end_ts,
        c.converted_ts,
        c.created_account_ts,
        s.session_id AS session_id,
        ROW_NUMBER() OVER (PARTITION BY c.blended_user_id ORDER BY s.session_start_ts) AS session_seq,
        count_conversions,
        CASE WHEN c.created_account_ts BETWEEN s.session_start_ts AND s.session_end_ts THEN TRUE ELSE FALSE END AS account_opening_session,
        CASE WHEN (c.converted_ts BETWEEN s.session_start_ts AND s.session_end_ts)  THEN TRUE ELSE FALSE END AS conversion_session,
        CASE WHEN (c.converted_ts BETWEEN s.session_start_ts AND s.session_end_ts)  THEN 1 ELSE 0 END AS conversion_event,
        CASE WHEN s.session_start_ts BETWEEN c.created_account_ts AND coalesce(c.converted_ts, s.session_end_ts) THEN TRUE ELSE FALSE END AS trialing_session,
        utm_source,
        utm_content,
        utm_medium,
        utm_campaign,
        referrer_host,
        first_page_url_host,
        split(net.reg_domain(referrer_host),'.')[OFFSET(0)] as referrer_domain,
        channel,
        events,
        order_id,
        total_revenue,
        currency_code
      FROM
        {{ ref('wh_web_sessions_fact') }} s
      JOIN
        converting_sessions_deduped c
      ON
        c.blended_user_id = s.blended_user_id
      WHERE
        c.converted_ts >= s.session_start_ts
      ORDER BY
        c.blended_user_id,
        s.session_start_ts)
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
    (session_day_number - max(session_day_number) over (partition by blended_user_id, user_conversion_cycle))*-1 <= {{ var('attribution_lookback_days_window') }} as is_within_attribution_lookback_window
  from
    converting_sessions_deduped_labelled_with_session_day_number
),

session_attrib_pct as (
    SELECT
      * except (first_page_url_host),
      CASE
        WHEN session_id = LAST_VALUE(if(is_within_attribution_lookback_window,session_id,null)  IGNORE NULLS) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)

        THEN 1
      ELSE
      0
    END
      AS LAST_click_attrib_pct,
      CASE
        WHEN session_id = FIRST_VALUE(if(is_within_attribution_lookback_window,session_id,null) IGNORE NULLS) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        THEN 1
      ELSE
      0
    END
      AS first_click_attrib_pct,
      IF(is_within_attribution_lookback_window,(1/COUNT(
          IF(is_within_attribution_lookback_window,session_id,null))
        OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)),0) AS even_click_attrib_pct,
      CASE
        WHEN session_start_ts = FIRST_VALUE(session_start_ts) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts) AND MAX(conversion_event) OVER (PARTITION BY blended_user_id, user_conversion_cycle) = 1 THEN SAFE_CAST(1.1-ROW_NUMBER() OVER (PARTITION BY blended_user_id, user_conversion_cycle) AS STRING)
        WHEN session_start_ts > LAG(session_start_ts) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts)
      AND MAX(conversion_event) OVER (PARTITION BY blended_user_id, user_conversion_cycle) = 1 THEN SAFE_CAST(ROUND(1.1-1/ROW_NUMBER() OVER (PARTITION BY blended_user_id, user_conversion_cycle), 2) AS STRING)
      ELSE
      'null'
    END
      AS weights
    FROM  days_to_each_conversion),
session_attrib_pct_with_time_decay AS (
    SELECT
      * EXCEPT (weights),
      ROUND(IF (SAFE_CAST(weights AS FLOAT64)=0 OR SUM(SAFE_CAST(weights AS FLOAT64)) OVER (PARTITION BY blended_user_id, user_conversion_cycle)=0, 0, SAFE_CAST(weights AS FLOAT64)/SUM(SAFE_CAST(weights AS FLOAT64)) OVER (PARTITION BY blended_user_id, user_conversion_cycle)), 2) AS time_decay_attrib_pct
    FROM
      session_attrib_pct),
final as (
    SELECT
      * ,
      round(MAX(count_conversions * first_click_attrib_pct),2) AS first_click_attrib_conversions,
      round(MAX(count_conversions * last_click_attrib_pct),2) AS last_click_attrib_conversions,
      round(MAX(count_conversions * even_click_attrib_pct),2) AS even_click_attrib_conversions,
      round(MAX(count_conversions * time_decay_attrib_pct),2) AS time_decay_attrib_conversions,
      round(MAX(total_revenue * first_click_attrib_pct),2) AS first_click_attrib_revenue,
      round(MAX(total_revenue * last_click_attrib_pct),2) AS last_click_attrib_revenue,
      round(MAX(total_revenue * even_click_attrib_pct),2) AS even_click_attrib_revenue,
      round(MAX(total_revenue * time_decay_attrib_pct),2) AS time_decay_attrib_revenue,
    FROM
      session_attrib_pct_with_time_decay
    {{ dbt_utils.group_by(32) }} )
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
  order_id,
  total_revenue,
  currency_code,
  events as session_events,
  user_conversion_cycle,
  days_before_conversion,
  is_within_attribution_lookback_window,
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
