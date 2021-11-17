{% if  var("marketing_warehouse_ad_campaign_sources") and var("product_warehouse_event_sources") %}
{% if target.type == 'snowflake' %}

{{
    config(
      alias='attribution_fact'
    )
}}

WITH
events_filtered as
  (SELECT
    *
  FROM (
    SELECT
      *,
      FIRST_VALUE(CASE WHEN event_type = '{{ var('attribution_create_account_event_type') }}' THEN event_id END IGNORE NULLS) OVER (PARTITION BY blended_user_id order by event_ts ROWS BETWEEN unbounded preceding AND unbounded following) AS first_registration_event_id,
      FIRST_VALUE(CASE WHEN event_type = '{{ var('attribution_conversion_event_type') }}' THEN event_id END IGNORE NULLS) OVER (PARTITION BY blended_user_id order by event_ts ROWS BETWEEN unbounded preceding AND unbounded following) AS first_order_event_id
    FROM
      {{ ref ('wh_web_events_fact') }})
  WHERE
    (event_type = '{{ var('attribution_conversion_event_type') }}'
    or (event_type = 'contact_us_pressed' and event_id = first_registration_event_id))
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

  ),
converting_sessions_deduped AS
  (
  SELECT
    session_id                                  AS session_id,
    MAX(blended_user_id)                        AS blended_user_id,
    SUM(first_order_total_revenue)              AS first_order_total_revenue,
    SUM(repeat_order_total_revenue)             AS repeat_order_total_revenue,
    MAX(currency_code)                          AS currency_code,
    SUM(count_first_order_conversions)          AS count_first_order_conversions,
    SUM(count_repeat_order_conversions)         AS count_repeat_order_conversions,
    SUM(count_order_conversions)                AS count_order_conversions,
    SUM(count_registration_conversions)         AS count_registration_conversions,
    SUM(count_registration_conversions)
    + SUM(count_first_order_conversions)
    + SUM(count_repeat_order_conversions)       AS count_conversions,
    MAX(converted_ts)                           AS converted_ts
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
            FIRST_VALUE(converted_ts ignore nulls) over (PARTITION BY blended_user_id ORDER BY session_start_ts rows BETWEEN current row AND unbounded following) as conversion_cycle_conversion_ts,
            ROW_NUMBER() OVER (PARTITION BY blended_user_id ORDER BY session_start_ts) AS session_seq
          FROM (
            SELECT
              s.blended_user_id                                                               AS blended_user_id,
              s.session_start_ts                                                              AS session_start_ts,
              s.session_end_ts                                                                AS session_end_ts,
              c.converted_ts                                                                  AS converted_ts,
              s.session_id                                                                    AS session_id,
              MAX(c.count_conversions)                                                        AS count_conversions,
              MAX(c.count_order_conversions)                                                  AS count_order_conversions,
              MAX(c.count_first_order_conversions)                                            AS count_first_order_conversions,
              MAX(c.count_repeat_order_conversions)                                           AS count_repeat_order_conversions,
              MAX(c.count_registration_conversions)                                           AS count_registration_conversions,
              COALESCE(CASE WHEN c.count_conversions >0 THEN TRUE ELSE FALSE END  ,false)     AS conversion_session,
              COALESCE(CASE WHEN c.count_conversions >0 THEN 1 ELSE 0 END  ,0)                AS conversion_event,
              COALESCE(CASE WHEN c.count_order_conversions>0 THEN 1 ELSE 0 END  ,0)           AS order_conversion_event,
              COALESCE(CASE WHEN c.count_registration_conversions>0 THEN 1 ELSE 0 END  ,0)    AS registration_conversion_event,
              COALESCE(CASE WHEN c.count_first_order_conversions>0 THEN 1 ELSE 0 END  ,0)     AS first_order_conversion_event,
              COALESCE(CASE WHEN c.count_repeat_order_conversions>0 THEN 1 ELSE 0 END  ,0)    AS repeat_order_conversion_event,
              utm_source                                                                      AS utm_source,
              utm_content                                                                     AS utm_content,
              utm_medium                                                                      AS utm_medium,
              utm_campaign                                                                    AS utm_campaign,
              referrer_host                                                                   AS referrer_host,
              first_page_url_host                                                             AS first_page_url_host,
              NULL                                                                            AS referrer_domain ,
              channel                                                                         AS channel,
              case when lower(channel) = 'direct' then false else true end                    AS is_non_direct_channel,
              case when lower(channel) like '%paid%' then true else false end                 AS is_paid_channel,
              events                                                                          AS events,
              c.first_order_total_revenue                                                     AS first_order_total_revenue,
              c.repeat_order_total_revenue                                                    AS repeat_order_total_revenue,
              c.currency_code                                                                 AS currency_code
            FROM
              {{ ref('wh_web_sessions_fact') }} s
            LEFT OUTER JOIN
              converting_sessions_deduped c
            ON
              s.session_id = c.session_id
            GROUP BY
              1,2,3,4,5,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30
        )
    )
  WHERE
    conversion_cycle_conversion_ts >= session_start_ts
),
converting_sessions_deduped_labelled_with_conversion_number AS
  (
    SELECT
      *,
      SUM(conversion_event) over (PARTITION BY blended_user_id ORDER BY session_start_ts rows BETWEEN unbounded preceding AND CURRENT ROW) AS user_total_conversions,
      SUM(count_order_conversions) over (PARTITION BY blended_user_id ORDER BY session_start_ts rows BETWEEN unbounded preceding AND CURRENT ROW) AS user_total_order_conversions,
      SUM(count_registration_conversions) over (PARTITION BY blended_user_id ORDER BY session_start_ts rows BETWEEN unbounded preceding AND CURRENT ROW) AS user_total_registration_conversions,
      SUM(count_first_order_conversions) over (PARTITION BY blended_user_id ORDER BY session_start_ts rows BETWEEN unbounded preceding AND CURRENT ROW) AS user_total_first_order_conversions,
      SUM(count_repeat_order_conversions) over (PARTITION BY blended_user_id ORDER BY session_start_ts rows BETWEEN unbounded preceding AND CURRENT ROW) AS user_total_repeat_order_conversions
    FROM
        converting_sessions_deduped_labelled
),
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
converting_sessions_deduped_labelled_with_session_day_number AS (
  SELECT
    *,
    DATEDIFF(DAY,'2018-01-01',session_start_ts) AS session_day_number
  FROM
    converting_sessions_deduped_labelled_with_conversion_cycles
),
days_to_each_conversion AS
(
  SELECT
    *,
    session_day_number - max(session_day_number) over (partition by blended_user_id, user_conversion_cycle)  AS days_before_conversion,
    (session_day_number - max(session_day_number) over (partition by blended_user_id, user_conversion_cycle))*-1 <= {{ var('attribution_lookback_days_window') }} as is_within_attribution_lookback_window,
    (session_day_number - max(session_day_number) over (partition by blended_user_id, user_conversion_cycle))*-1 <= {{ var('attribution_time_decay_days_window') }} as is_within_attribution_time_decay_days_window
  FROM
    converting_sessions_deduped_labelled_with_session_day_number
),
add_time_decay_score AS (
  SELECT
    *,
    {{ iff() }} (is_within_attribution_time_decay_days_window,{{ safe_divide('POW(2,days_before_conversion-1)',var('attribution_time_decay_days_window')  ) }} ,null) AS time_decay_score,
    {{ iff() }} (conversion_session AND NOT TRUE,0,pow(2, (days_before_conversion - 1))) AS weighting,
    {{ iff() }} (conversion_session AND NOT TRUE,0,(COUNT(CASE WHEN NOT conversion_session OR TRUE THEN session_id END) over (PARTITION BY blended_user_id,DATE_TRUNC('day', CAST(session_start_ts AS DATE))))) AS sessions_within_day_to_conversion,
    {{ iff() }} (conversion_session AND NOT TRUE,0,div0 (pow(2, (days_before_conversion - 1)), COUNT(CASE WHEN NOT conversion_session OR TRUE THEN session_id END) over (PARTITION BY blended_user_id, DATE_TRUNC('day', CAST(session_start_ts AS DATE))))) AS weighting_split_by_days_sessions
  FROM
    days_to_each_conversion
),
split_time_decay_score_across_days_sessions as (
  select
    *,
    {{safe_divide('time_decay_score','sessions_within_day_to_conversion') }} as apportioned_time_decay_score
  from
    add_time_decay_score
),
session_attrib_pct AS (
  SELECT
    * ,
    {{ iff() }}(conversion_session and not {{ var('attribution_include_conversion_session') }},0,CASE
    WHEN session_id = LAST_VALUE({{ iff() }}(is_within_attribution_lookback_window and (not conversion_session or {{ var('attribution_include_conversion_session') }}),session_id,null)  IGNORE NULLS) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) THEN 1
      ELSE 0
    END) AS last_click_attrib_pct,
  {{ iff() }}(conversion_session and not {{ var('attribution_include_conversion_session') }},0,CASE
    WHEN session_id = LAST_VALUE({{ iff() }}(is_within_attribution_lookback_window and (not conversion_session or {{ var('attribution_include_conversion_session') }}) and is_non_direct_channel,session_id,null)  IGNORE NULLS) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) THEN 1
      ELSE 0
    END) AS last_non_direct_click_attrib_pct,
  {{ iff() }}(conversion_session and not {{ var('attribution_include_conversion_session') }},0,CASE
    WHEN session_id = LAST_VALUE({{ iff() }}(is_within_attribution_lookback_window and (not conversion_session or {{ var('attribution_include_conversion_session') }}) and is_paid_channel,session_id,null)  IGNORE NULLS) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) THEN 1
      ELSE 0
    END) AS last_paid_click_attrib_pct,
  {{ iff() }}(conversion_session and not {{ var('attribution_include_conversion_session') }},0,CASE
    WHEN session_id = FIRST_VALUE({{ iff() }}(is_within_attribution_lookback_window,session_id,null) IGNORE NULLS) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) THEN 1
      ELSE 0
    END) AS first_click_attrib_pct,
  {{ iff() }}(conversion_session and not {{ var('attribution_include_conversion_session') }},0,CASE
    WHEN session_id = FIRST_VALUE({{ iff() }}(is_within_attribution_lookback_window and (not conversion_session or {{ var('attribution_include_conversion_session') }}) and is_non_direct_channel,session_id,null) IGNORE NULLS) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) THEN 1
      ELSE 0
    END)
  AS first_non_direct_click_attrib_pct,
  {{ iff() }}(conversion_session and not {{ var('attribution_include_conversion_session') }},0,CASE
    WHEN session_id = FIRST_VALUE({{ iff() }}(is_within_attribution_lookback_window  and (not conversion_session or {{ var('attribution_include_conversion_session') }}) and is_paid_channel,session_id,null) IGNORE NULLS) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) THEN 1
      ELSE 0
    END)
    AS first_paid_click_attrib_pct,
  {{ iff() }} (conversion_session AND NOT TRUE,0,
    {{ iff() }} (is_within_attribution_lookback_window,(div0 (1,(COUNT({{ iff() }}(is_within_attribution_lookback_window,session_id,NULL)) over (PARTITION BY blended_user_id,user_conversion_cycle ORDER BY session_start_ts rows BETWEEN unbounded preceding AND unbounded following) + 0))),0)
    ) AS even_click_attrib_pct,
  {{ iff() }} (conversion_session AND NOT TRUE,0,CASE WHEN is_within_attribution_time_decay_days_window THEN apportioned_time_decay_score / NULLIF((SUM(apportioned_time_decay_score) over (PARTITION BY blended_user_id, user_conversion_cycle)),0) END
    ) AS time_decay_attrib_pct
  FROM
    split_time_decay_score_across_days_sessions
),
FINAL AS (
  SELECT
    *,
    (MAX(count_registration_conversions) over (PARTITION BY blended_user_id, user_conversion_cycle) * first_click_attrib_pct) AS user_registration_first_click_attrib_conversions,
    (MAX(count_registration_conversions) over (PARTITION BY blended_user_id, user_conversion_cycle) * first_non_direct_click_attrib_pct) AS user_registration_first_non_direct_click_attrib_conversions,
    (MAX(count_registration_conversions) over (PARTITION BY blended_user_id, user_conversion_cycle) * first_paid_click_attrib_pct) AS user_registration_first_paid_click_attrib_conversions,
    (MAX(count_registration_conversions) over (PARTITION BY blended_user_id, user_conversion_cycle) * last_click_attrib_pct) AS user_registration_last_click_attrib_conversions,
    (MAX(count_registration_conversions) over (PARTITION BY blended_user_id, user_conversion_cycle) * last_non_direct_click_attrib_pct) AS user_registration_last_non_direct_click_attrib_conversions,
    (MAX(count_registration_conversions) over (PARTITION BY blended_user_id, user_conversion_cycle) * last_paid_click_attrib_pct) AS user_registration_last_paid_click_attrib_conversions,
    (MAX(count_registration_conversions) over (PARTITION BY blended_user_id, user_conversion_cycle) * even_click_attrib_pct) AS user_registration_even_click_attrib_conversions,
    (MAX(count_registration_conversions) over (PARTITION BY blended_user_id, user_conversion_cycle) * time_decay_attrib_pct) AS user_registration_time_decay_attrib_conversions,
    (MAX(count_first_order_conversions) over (PARTITION BY blended_user_id, user_conversion_cycle) * first_click_attrib_pct) AS first_order_first_click_attrib_conversions,
    (MAX(count_first_order_conversions) over (PARTITION BY blended_user_id, user_conversion_cycle) * first_non_direct_click_attrib_pct) AS first_order_first_non_direct_click_attrib_conversions,
    (MAX(count_first_order_conversions) over (PARTITION BY blended_user_id, user_conversion_cycle) * first_paid_click_attrib_pct) AS first_order_first_paid_click_attrib_conversions,
    (MAX(count_first_order_conversions) over (PARTITION BY blended_user_id, user_conversion_cycle) * last_click_attrib_pct) AS first_order_last_click_attrib_conversions,
    (MAX(count_first_order_conversions) over (PARTITION BY blended_user_id, user_conversion_cycle) * last_non_direct_click_attrib_pct) AS first_order_last_non_direct_click_attrib_conversions,
    (MAX(count_first_order_conversions) over (PARTITION BY blended_user_id, user_conversion_cycle) * last_paid_click_attrib_pct) AS first_order_last_paid_click_attrib_conversions,
    (MAX(count_first_order_conversions) over (PARTITION BY blended_user_id, user_conversion_cycle) * even_click_attrib_pct) AS first_order_even_click_attrib_conversions,
    (MAX(count_first_order_conversions) over (PARTITION BY blended_user_id, user_conversion_cycle) * time_decay_attrib_pct) AS first_order_time_decay_attrib_conversions,
    (MAX(first_order_total_revenue) over (PARTITION BY blended_user_id, user_conversion_cycle) * first_click_attrib_pct) AS first_order_first_click_attrib_revenue,
    (MAX(first_order_total_revenue) over (PARTITION BY blended_user_id, user_conversion_cycle) * first_non_direct_click_attrib_pct) AS first_order_first_non_direct_click_attrib_revenue,
    (MAX(first_order_total_revenue) over (PARTITION BY blended_user_id, user_conversion_cycle) * first_paid_click_attrib_pct) AS first_order_first_paid_click_attrib_revenue,
    (MAX(first_order_total_revenue) over (PARTITION BY blended_user_id, user_conversion_cycle) * last_click_attrib_pct) AS first_order_last_click_attrib_revenue,
    (MAX(first_order_total_revenue) over (PARTITION BY blended_user_id, user_conversion_cycle) * last_non_direct_click_attrib_pct) AS first_order_last_non_direct_click_attrib_revenue,
    (MAX(first_order_total_revenue) over (PARTITION BY blended_user_id, user_conversion_cycle) * last_paid_click_attrib_pct) AS first_order_last_paid_click_attrib_revenue,
    (MAX(first_order_total_revenue) over (PARTITION BY blended_user_id, user_conversion_cycle) * even_click_attrib_pct) AS first_order_even_click_attrib_revenue,
    (MAX(first_order_total_revenue) over (PARTITION BY blended_user_id, user_conversion_cycle) * time_decay_attrib_pct) AS first_order_time_decay_attrib_revenue,
    (MAX(count_repeat_order_conversions) over (PARTITION BY blended_user_id, user_conversion_cycle) * first_click_attrib_pct) AS repeat_order_first_click_attrib_conversions,
    (MAX(count_repeat_order_conversions) over (PARTITION BY blended_user_id, user_conversion_cycle) * first_non_direct_click_attrib_pct) AS repeat_order_first_non_direct_click_attrib_conversions,
    (MAX(count_repeat_order_conversions) over (PARTITION BY blended_user_id, user_conversion_cycle) * first_paid_click_attrib_pct) AS repeat_order_first_paid_click_attrib_conversions,
    (MAX(count_repeat_order_conversions) over (PARTITION BY blended_user_id, user_conversion_cycle) * last_click_attrib_pct) AS repeat_order_last_click_attrib_conversions,
    (MAX(count_repeat_order_conversions) over (PARTITION BY blended_user_id, user_conversion_cycle) * last_non_direct_click_attrib_pct) AS repeat_order_last_non_direct_click_attrib_conversions,
    (MAX(count_repeat_order_conversions) over (PARTITION BY blended_user_id, user_conversion_cycle) * last_paid_click_attrib_pct) AS repeat_order_last_paid_click_attrib_conversions,
    (MAX(count_repeat_order_conversions) over (PARTITION BY blended_user_id, user_conversion_cycle) * even_click_attrib_pct) AS repeat_order_even_click_attrib_conversions,
    (MAX(count_repeat_order_conversions) over (PARTITION BY blended_user_id, user_conversion_cycle) * time_decay_attrib_pct) AS repeat_order_time_decay_attrib_conversions,
    (MAX(repeat_order_total_revenue) over (PARTITION BY blended_user_id, user_conversion_cycle) * first_click_attrib_pct) AS repeat_order_first_click_attrib_revenue,
    (MAX(repeat_order_total_revenue) over (PARTITION BY blended_user_id, user_conversion_cycle) * first_non_direct_click_attrib_pct) AS repeat_order_first_non_direct_click_attrib_revenue,
    (MAX(repeat_order_total_revenue) over (PARTITION BY blended_user_id, user_conversion_cycle) * first_paid_click_attrib_pct) AS repeat_order_first_paid_click_attrib_revenue,
    (MAX(repeat_order_total_revenue) over (PARTITION BY blended_user_id, user_conversion_cycle) * last_click_attrib_pct) AS repeat_order_last_click_attrib_revenue,
    (MAX(repeat_order_total_revenue) over (PARTITION BY blended_user_id, user_conversion_cycle) * last_non_direct_click_attrib_pct) AS repeat_order_last_non_direct_click_attrib_revenue,
    (MAX(repeat_order_total_revenue) over (PARTITION BY blended_user_id, user_conversion_cycle) * last_paid_click_attrib_pct) AS repeat_order_last_paid_click_attrib_revenue,
    (MAX(repeat_order_total_revenue) over (PARTITION BY blended_user_id, user_conversion_cycle) * even_click_attrib_pct) AS repeat_order_even_click_attrib_revenue,
    (MAX(repeat_order_total_revenue) over (PARTITION BY blended_user_id, user_conversion_cycle) * time_decay_attrib_pct) AS repeat_order_time_decay_attrib_revenue
  FROM
    session_attrib_pct
  {{ dbt_utils.group_by(58) }}
)
SELECT
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
  weighting AS time_decay_score_weighting,
  weighting_split_by_days_sessions AS time_decay_weighting_split_by_days_sessions,
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
FROM
  FINAL
{% endif %}
{% endif %}
